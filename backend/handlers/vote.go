package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// Vote persists or updates a user's prediction for a match in Postgres.
// Expects Authorization: Bearer <token> (middleware sets user_id) and JSON { winner: <rikishi_id> }.
func (a *App) Vote(c *gin.Context) {
	// user_id set by JWTMiddleware
	ui, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	userID, _ := ui.(string)

	matchIDStr := c.Param("id")
	matchID, err := strconv.ParseInt(matchIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid match id"})
		return
	}

	var body struct {
		Winner int64 `json:"winner"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Upsert in one statement using the unique constraint on (user_id, match_id).
	// RETURNING (xmax = 0) as inserted is a common trick: inserted rows have xmax = 0.
	var inserted bool
	query := `
        INSERT INTO match_predictions (user_id, match_id, predicted_winner, prediction_time)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (user_id, match_id) DO UPDATE
          SET predicted_winner = EXCLUDED.predicted_winner,
              prediction_time = NOW()
        RETURNING (xmax = 0) as inserted
    `
	err = a.PG.QueryRow(ctx, query, userID, matchID, body.Winner).Scan(&inserted)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save prediction"})
		return
	}
	if inserted {
		c.JSON(http.StatusCreated, gin.H{"message": "prediction saved"})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": "prediction updated"})
	}
}
