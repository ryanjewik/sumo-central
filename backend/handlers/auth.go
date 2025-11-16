package handlers

import (
	"context"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

// Register accepts JSON {username,email,password} and creates a user in Postgres.
func (a *App) Register(c *gin.Context) {
	var body struct {
		Username string `json:"username" binding:"required"`
		Email    string `json:"email" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	username := body.Username
	email := body.Email
	password := body.Password

	// simple email validation (match messages used in the Flask implementation)
	emailRegex := regexp.MustCompile(`^[\w\.-]+@[\w\.-]+\.\w+$`)
	if !emailRegex.MatchString(email) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid email format."})
		return
	}

	// Password rules (mirror the Flask rules): at least 10 chars, 1 uppercase, 1 number
	if len(password) < 10 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password must be at least 10 characters."})
		return
	}
	if !regexp.MustCompile(`[A-Z]`).MatchString(password) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password must contain at least one uppercase letter."})
		return
	}
	if !regexp.MustCompile(`\d`).MatchString(password) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password must contain at least one number."})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// check username/email exists
	var exists bool
	err := a.PG.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE username=$1)", username).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username already taken"})
		return
	}
	err = a.PG.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE email=$1)", email).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email already registered"})
		return
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash"})
		return
	}

	userID := uuid.NewString()
	_, err = a.PG.Exec(ctx, "INSERT INTO users (id, username, email, password, created_at) VALUES ($1,$2,$3,$4,NOW())", userID, username, email, string(hashed))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create user"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"success": true, "id": userID})
}

// Login accepts JSON {username,password}. On success returns a JWT token and user info.
func (a *App) Login(c *gin.Context) {
	var body struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	username := body.Username
	password := body.Password

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	var userID string
	var usernameDb string
	var hashed string
	err := a.PG.QueryRow(ctx, "SELECT id, username, password FROM users WHERE username=$1", username).Scan(&userID, &usernameDb, &hashed)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid username or password"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	if bcrypt.CompareHashAndPassword([]byte(hashed), []byte(password)) != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid username or password"})
		return
	}

	// create JWT
	if a.Cfg.JWTSecret == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "auth not configured"})
		return
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":  userID,
		"name": usernameDb,
		"exp":  time.Now().Add(24 * time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte(a.Cfg.JWTSecret))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sign token"})
		return
	}

	// Create and persist a refresh token (rotateable)
	refreshToken := uuid.NewString()
	refreshExpiry := time.Now().Add(14 * 24 * time.Hour)
	// reuse ctx from above
	_, err = a.PG.Exec(ctx, "INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES ($1,$2,$3)", refreshToken, userID, refreshExpiry)
	if err != nil {
		// token storage failure shouldn't block login entirely, but return an error
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create refresh token"})
		return
	}

	// Set refresh token as HttpOnly cookie. Secure if TLS present; SameSite=Lax for general safety.
	cookie := &http.Cookie{
		Name:     "refresh_token",
		Value:    refreshToken,
		Path:     "/",
		Expires:  refreshExpiry,
		HttpOnly: true,
		Secure:   c.Request.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	}
	http.SetCookie(c.Writer, cookie)

	c.JSON(http.StatusOK, gin.H{"token": signed, "id": userID, "username": usernameDb})
}

// Refresh issues a new access token when a valid refresh cookie is presented.
func (a *App) Refresh(c *gin.Context) {
	// Read refresh cookie
	rt, err := c.Cookie("refresh_token")
	if err != nil || rt == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "no refresh token"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Validate refresh token in DB: not revoked and not expired. Also fetch username
	var userID string
	var expires time.Time
	var username string
	err = a.PG.QueryRow(ctx, "SELECT rt.user_id, rt.expires_at, u.username FROM refresh_tokens rt JOIN users u ON u.id = rt.user_id WHERE rt.token=$1 AND rt.revoked=false", rt).Scan(&userID, &expires, &username)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid refresh token"})
		return
	}
	if time.Now().After(expires) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "refresh token expired"})
		return
	}

	// Rotate: mark current token revoked and issue a new one
	newToken := uuid.NewString()
	newExpiry := time.Now().Add(14 * 24 * time.Hour)
	tx, err := a.PG.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "UPDATE refresh_tokens SET revoked=true WHERE token=$1", rt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	_, err = tx.Exec(ctx, "INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES ($1,$2,$3)", newToken, userID, newExpiry)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	if err := tx.Commit(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	// Issue new access token (include username so client can rehydrate display)
	accessTok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":  userID,
		"name": username,
		"exp":  time.Now().Add(1 * time.Hour).Unix(),
	})
	signed, err := accessTok.SignedString([]byte(a.Cfg.JWTSecret))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sign token"})
		return
	}

	// Set rotated refresh cookie
	cookie := &http.Cookie{
		Name:     "refresh_token",
		Value:    newToken,
		Path:     "/",
		Expires:  newExpiry,
		HttpOnly: true,
		Secure:   c.Request.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	}
	http.SetCookie(c.Writer, cookie)

	c.JSON(http.StatusOK, gin.H{"access_token": signed})
}

// Logout revokes the current refresh token and clears the cookie.
func (a *App) Logout(c *gin.Context) {
	rt, err := c.Cookie("refresh_token")
	if err == nil && rt != "" {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		_, _ = a.PG.Exec(ctx, "UPDATE refresh_tokens SET revoked=true WHERE token=$1", rt)
	}
	// Clear cookie
	cookie := &http.Cookie{
		Name:     "refresh_token",
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		Secure:   c.Request.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	}
	http.SetCookie(c.Writer, cookie)
	c.JSON(http.StatusOK, gin.H{"message": "logged out"})
}

// Auth validates the Bearer token from Authorization header and returns claims if valid.
func (a *App) Auth(c *gin.Context) {
	auth := c.GetHeader("Authorization")
	if auth == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing Authorization header"})
		return
	}
	// expect "Bearer <token>"
	var tokenStr string
	if n, _ := regexp.MatchString(`^Bearer\s+`, auth); n {
		tokenStr = auth[len("Bearer "):]
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Authorization header"})
		return
	}

	t, err := jwt.Parse(tokenStr, func(tok *jwt.Token) (interface{}, error) {
		if tok.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, jwt.ErrTokenSignatureInvalid
		}
		return []byte(a.Cfg.JWTSecret), nil
	})
	if err != nil || !t.Valid {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"claims": t.Claims})
}

// JWTMiddleware validates the Authorization header (Bearer token), verifies the JWT,
// and stores the subject (user id) in the Gin context under the key "user_id".
func (a *App) JWTMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		auth := c.GetHeader("Authorization")
		if auth == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing Authorization header"})
			return
		}
		// expect "Bearer <token>"
		var tokenStr string
		if n, _ := regexp.MatchString(`^Bearer\s+`, auth); n {
			tokenStr = auth[len("Bearer "):]
		} else {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid Authorization header"})
			return
		}

		t, err := jwt.Parse(tokenStr, func(tok *jwt.Token) (interface{}, error) {
			if tok.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, jwt.ErrTokenSignatureInvalid
			}
			return []byte(a.Cfg.JWTSecret), nil
		})
		if err != nil || !t.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		// Extract subject claim and optional username
		if claims, ok := t.Claims.(jwt.MapClaims); ok {
			if sub, ok := claims["sub"].(string); ok && sub != "" {
				c.Set("user_id", sub)
				if name, ok2 := claims["name"].(string); ok2 && name != "" {
					c.Set("username", name)
				}
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token claims"})
	}
}
