package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Leaderboard returns the top users ordered by correct_predictions (desc), up to 10 rows.
func (a *App) Leaderboard(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	rows, err := a.PG.Query(ctx, "SELECT username, correct_predictions FROM users ORDER BY correct_predictions DESC LIMIT 10")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer rows.Close()

	type entry struct {
		Username           string `json:"username"`
		CorrectPredictions int    `json:"correctPredictions"`
	}

	var out []entry
	for rows.Next() {
		var u string
		var cp int
		if err := rows.Scan(&u, &cp); err != nil {
			continue
		}
		out = append(out, entry{Username: u, CorrectPredictions: cp})
	}

	c.JSON(http.StatusOK, gin.H{"leaderboard": out})
}
