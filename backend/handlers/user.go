package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

func (a *App) GetUser(c *gin.Context) {
	// Handler supports two modes:
	// 1) GET /users/:id  - fetch a user by the provided path param
	// 2) GET /users/me   - fetch the current authenticated user; this requires
	//    that authentication middleware has previously set a context key
	//    named "userID" (or you can parse the Authorization Bearer token here).

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Prefer explicit path parameter
	id := c.Param("id")

	// If caller used "/users/me" or omitted the id, try to obtain id from gin context.
	// Note: our JWT middleware sets the key "user_id" (see handlers/auth.go).
	if id == "" || id == "me" {
		// check common keys middleware might set
		if v, ok := c.Get("user_id"); ok {
			id = fmt.Sprintf("%v", v)
		} else if v, ok := c.Get("userID"); ok {
			id = fmt.Sprintf("%v", v)
		}
	}

	if id == "" {
		// No id available: either the client should provide /users/:id or you must
		// ensure your authentication middleware sets a context value named "userID".
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing user id; authenticate or include /users/:id"})
		return
	}

	// Determine if this request is for the authenticated user (so we can include email)
	_, authed := c.Get("user_id")

	// Query Postgres for the user row with the fields requested by the frontend
	// (username, created_at, predictions_ratio, favorite_rikishi, num_posts,
	//  num_predictions, correct_predictions, false_predictions, country)
	type outUser struct {
		ID                 string
		Username           string
		CreatedAt          time.Time
		PredictionsRatio   sql.NullFloat64
		FavoriteRikishi    sql.NullInt64
		NumPosts           sql.NullInt64
		NumPredictions     sql.NullInt64
		CorrectPredictions sql.NullInt64
		FalsePredictions   sql.NullInt64
		Country            sql.NullString
	}

	var u outUser
	// include email in the query; we'll only include it in the JSON response
	row := a.PG.QueryRow(ctx, `SELECT id, username, email, created_at, predictions_ratio, favorite_rikishi, num_posts, num_predictions, correct_predictions, false_predictions, country FROM users WHERE id = $1`, id)
	var email sql.NullString
	if err := row.Scan(&u.ID, &u.Username, &email, &u.CreatedAt, &u.PredictionsRatio, &u.FavoriteRikishi, &u.NumPosts, &u.NumPredictions, &u.CorrectPredictions, &u.FalsePredictions, &u.Country); err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	resp := gin.H{
		"id":        u.ID,
		"username":  u.Username,
		"createdAt": u.CreatedAt,
	}
	if u.PredictionsRatio.Valid {
		resp["predictions_ratio"] = u.PredictionsRatio.Float64
	} else {
		resp["predictions_ratio"] = nil
	}
	if u.FavoriteRikishi.Valid {
		// try to fetch rikishi summary from rikishi table to provide useful
		// public display fields. If the rikishi table or row is missing, fall
		// back to returning the raw id string.
		var rk struct {
			ID           sql.NullInt64
			Shikona      sql.NullString
			CurrentRank  sql.NullString
			Heya         sql.NullString
			Wins         sql.NullInt64
			Losses       sql.NullInt64
			Matches      sql.NullInt64
			YushoCount   sql.NullInt64
			SanshoCount  sql.NullInt64
		}
		// attempt to select from rikishi; columns may vary across DBs
		err := a.PG.QueryRow(ctx, `SELECT id, shikona, current_rank, heya, wins, losses, matches, yusho_count, sansho_count FROM rikishi WHERE id = $1`, u.FavoriteRikishi.Int64).Scan(&rk.ID, &rk.Shikona, &rk.CurrentRank, &rk.Heya, &rk.Wins, &rk.Losses, &rk.Matches, &rk.YushoCount, &rk.SanshoCount)
		if err == nil {
			fav := gin.H{"id": rk.ID.Int64}
			if rk.Shikona.Valid { fav["shikona"] = rk.Shikona.String }
			if rk.CurrentRank.Valid { fav["current_rank"] = rk.CurrentRank.String }
			if rk.Heya.Valid { fav["heya"] = rk.Heya.String }
			if rk.Wins.Valid { fav["wins"] = rk.Wins.Int64 }
			if rk.Losses.Valid { fav["losses"] = rk.Losses.Int64 }
			if rk.Matches.Valid { fav["matches"] = rk.Matches.Int64 }
			if rk.YushoCount.Valid { fav["yusho_count"] = rk.YushoCount.Int64 }
			if rk.SanshoCount.Valid { fav["sansho_count"] = rk.SanshoCount.Int64 }
			resp["favorite_rikishi"] = fav
		} else {
			// fallback to id (int)
			resp["favorite_rikishi"] = u.FavoriteRikishi.Int64
		}
	} else {
		resp["favorite_rikishi"] = nil
	}
	// numeric counts
	resp["num_posts"] = 0
	if u.NumPosts.Valid {
		resp["num_posts"] = u.NumPosts.Int64
	}
	resp["num_predictions"] = 0
	if u.NumPredictions.Valid {
		resp["num_predictions"] = u.NumPredictions.Int64
	}
	resp["correct_predictions"] = 0
	if u.CorrectPredictions.Valid {
		resp["correct_predictions"] = u.CorrectPredictions.Int64
	}
	resp["false_predictions"] = 0
	if u.FalsePredictions.Valid {
		resp["false_predictions"] = u.FalsePredictions.Int64
	}
	if u.Country.Valid {
		resp["country"] = u.Country.String
	} else {
		resp["country"] = nil
	}

	// Only include email in the JSON response if this is an authenticated /me request
	if authed && email.Valid {
		resp["email"] = email.String
	}

	c.JSON(http.StatusOK, resp)
}

// UpdateMe allows the authenticated user to update their username, password, and country.
// Requires JWT middleware to have stored "user_id" in the gin.Context.
func (a *App) UpdateMe(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Ensure we have an authenticated user id from middleware
	v, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	userID := fmt.Sprintf("%v", v)

	// Parse input
	var body struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		PasswordConfirm string `json:"password_confirm"`
		CurrentPassword string `json:"current_password"`
		CurrentUsername string `json:"current_username"`
		Country         string `json:"country"`
		FavoriteRikishi string `json:"favorite_rikishi"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// Validate password rules if provided
	if body.Password != "" {
		if len(body.Password) < 10 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Password must be at least 10 characters."})
			return
		}
		if matched, _ := regexp.MatchString(`[A-Z]`, body.Password); !matched {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Password must contain at least one uppercase letter."})
			return
		}
		if matched, _ := regexp.MatchString(`\d`, body.Password); !matched {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Password must contain at least one number."})
			return
		}
	}

	// Start transaction to make updates atomic
	tx, err := a.PG.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer tx.Rollback(ctx)

	// Fetch stored username and password hash once (if needed)
	var storedHash string
	var storedUsername string
	if body.Username != "" || body.Password != "" || body.CurrentPassword != "" || body.CurrentUsername != "" {
		if err := tx.QueryRow(ctx, "SELECT username, password FROM users WHERE id=$1", userID).Scan(&storedUsername, &storedHash); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
	}

	// If username supplied, require current_password and verify it matches before changing
	if body.Username != "" {
		// require current_password in body
		if body.CurrentPassword == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "current password required to change username"})
			return
		}
		// verify provided current username (if supplied) matches stored username
		if body.CurrentUsername != "" && body.CurrentUsername != storedUsername {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "current username does not match"})
			return
		}
		// verify current password matches stored hash
		if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(body.CurrentPassword)) != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "current password incorrect"})
			return
		}

		var exists bool
		err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE username=$1 AND id <> $2)", body.Username, userID).Scan(&exists)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
		if exists {
			c.JSON(http.StatusBadRequest, gin.H{"error": "username already taken"})
			return
		}
		if _, err := tx.Exec(ctx, "UPDATE users SET username=$1 WHERE id=$2", body.Username, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
	}

	// If password supplied, require current username and password and then hash and update
	if body.Password != "" {
		// require current password and current username
		if body.CurrentPassword == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "current password required to change password"})
			return
		}
		if body.CurrentUsername == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "current username required to change password"})
			return
		}
		// verify provided current username matches stored
		if body.CurrentUsername != storedUsername {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "current username does not match"})
			return
		}
		// verify password
		if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(body.CurrentPassword)) != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "current password incorrect"})
			return
		}

		// password confirm handled client-side; server will still validate rules
		hashed, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
			return
		}
		if _, err := tx.Exec(ctx, "UPDATE users SET password=$1 WHERE id=$2", string(hashed), userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
	}

	// Country update (allow empty string to clear)
	if body.Country != "" {
		if _, err := tx.Exec(ctx, "UPDATE users SET country=$1 WHERE id=$2", body.Country, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
	}

	// Favorite rikishi update (accept id as string/int). Validate exists in
	// rikishi table before setting to ensure FK integrity.
	if body.FavoriteRikishi != "" {
		// parse to int64
		fid, perr := strconv.ParseInt(body.FavoriteRikishi, 10, 64)
		if perr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid favorite_rikishi id"})
			return
		}
		// attempt to find rikishi row by id
		var exists bool
		err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM rikishi WHERE id=$1)", fid).Scan(&exists)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
		if !exists {
			c.JSON(http.StatusBadRequest, gin.H{"error": "favorite rikishi not found"})
			return
		}
		if _, err := tx.Exec(ctx, "UPDATE users SET favorite_rikishi=$1 WHERE id=$2", fid, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	// Return updated profile (include email for /me)
	var out struct {
		ID                 string          `json:"id"`
		Username           string          `json:"username"`
		Email              sql.NullString  `json:"email"`
		CreatedAt          time.Time       `json:"createdAt"`
		PredictionsRatio   sql.NullFloat64 `json:"predictions_ratio"`
		FavoriteRikishi    sql.NullInt64   `json:"favorite_rikishi"`
		NumPosts           sql.NullInt64   `json:"num_posts"`
		NumPredictions     sql.NullInt64   `json:"num_predictions"`
		CorrectPredictions sql.NullInt64   `json:"correct_predictions"`
		FalsePredictions   sql.NullInt64   `json:"false_predictions"`
		Country            sql.NullString  `json:"country"`
	}

	row := a.PG.QueryRow(ctx, `SELECT id, username, email, created_at, predictions_ratio, favorite_rikishi, num_posts, num_predictions, correct_predictions, false_predictions, country FROM users WHERE id = $1`, userID)
	if err := row.Scan(&out.ID, &out.Username, &out.Email, &out.CreatedAt, &out.PredictionsRatio, &out.FavoriteRikishi, &out.NumPosts, &out.NumPredictions, &out.CorrectPredictions, &out.FalsePredictions, &out.Country); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	resp := gin.H{"id": out.ID, "username": out.Username, "createdAt": out.CreatedAt}
	if out.Email.Valid {
		resp["email"] = out.Email.String
	}
	if out.PredictionsRatio.Valid {
		resp["predictions_ratio"] = out.PredictionsRatio.Float64
	} else {
		resp["predictions_ratio"] = nil
	}
	if out.FavoriteRikishi.Valid {
		// attempt to also return rikishi summary for convenience
		var rk struct {
			ID           sql.NullInt64
			Shikona      sql.NullString
			CurrentRank  sql.NullString
			Heya         sql.NullString
			Wins         sql.NullInt64
			Losses       sql.NullInt64
			Matches      sql.NullInt64
			YushoCount   sql.NullInt64
			SanshoCount  sql.NullInt64
		}
		err := a.PG.QueryRow(ctx, `SELECT id, shikona, current_rank, heya, wins, losses, matches, yusho_count, sansho_count FROM rikishi WHERE id = $1`, out.FavoriteRikishi.Int64).Scan(&rk.ID, &rk.Shikona, &rk.CurrentRank, &rk.Heya, &rk.Wins, &rk.Losses, &rk.Matches, &rk.YushoCount, &rk.SanshoCount)
		if err == nil {
			fav := gin.H{"id": rk.ID.Int64}
			if rk.Shikona.Valid { fav["shikona"] = rk.Shikona.String }
			if rk.CurrentRank.Valid { fav["current_rank"] = rk.CurrentRank.String }
			if rk.Heya.Valid { fav["heya"] = rk.Heya.String }
			if rk.Wins.Valid { fav["wins"] = rk.Wins.Int64 }
			if rk.Losses.Valid { fav["losses"] = rk.Losses.Int64 }
			if rk.Matches.Valid { fav["matches"] = rk.Matches.Int64 }
			if rk.YushoCount.Valid { fav["yusho_count"] = rk.YushoCount.Int64 }
			if rk.SanshoCount.Valid { fav["sansho_count"] = rk.SanshoCount.Int64 }
			resp["favorite_rikishi"] = fav
		} else {
			resp["favorite_rikishi"] = out.FavoriteRikishi.Int64
		}
	} else {
		resp["favorite_rikishi"] = nil
	}
	resp["num_posts"] = 0
	if out.NumPosts.Valid {
		resp["num_posts"] = out.NumPosts.Int64
	}
	resp["num_predictions"] = 0
	if out.NumPredictions.Valid {
		resp["num_predictions"] = out.NumPredictions.Int64
	}
	resp["correct_predictions"] = 0
	if out.CorrectPredictions.Valid {
		resp["correct_predictions"] = out.CorrectPredictions.Int64
	}
	resp["false_predictions"] = 0
	if out.FalsePredictions.Valid {
		resp["false_predictions"] = out.FalsePredictions.Int64
	}
	if out.Country.Valid {
		resp["country"] = out.Country.String
	} else {
		resp["country"] = nil
	}

	c.JSON(http.StatusOK, resp)
}
