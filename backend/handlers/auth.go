// backend/handlers/auth.go
package handlers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type registerReq struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8"`
}

func (a *App) Register(c *gin.Context) {
	var req registerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}

	// hash password
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "hash error"})
		return
	}

	_, err = a.PG.Exec(
		context.Background(),
		"INSERT INTO users (email, password_hash) VALUES ($1, $2)",
		req.Email, string(hash),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db insert failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "registered"})
}
