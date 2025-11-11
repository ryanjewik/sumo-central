package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// GetRikishi returns a rikishi page document from the `rikishi_pages` collection
// It looks up the document by the `id` field (string) matching the route param.
func (a *App) GetRikishi(c *gin.Context) {
	id := c.Param("id")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// ID is stored as an integer in Mongo. Parse and only query numeric types.
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id must be numeric"})
		return
	}
	filter := bson.M{"$or": bson.A{bson.M{"id": n}, bson.M{"id": int32(n)}}}

	coll := a.Mongo.Collection("rikishi_pages")
	var doc bson.M
	err = coll.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "rikishi not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch rikishi"})
		return
	}

	c.JSON(http.StatusOK, doc)
}

// ListRikishi returns a list of rikishi ids from Postgres (table: rikishi)
func (a *App) ListRikishi(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// select id + shikona (display name) when available
	rows, err := a.PG.Query(ctx, "SELECT id, shikona FROM rikishi ORDER BY id")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer rows.Close()

	var items []gin.H
	for rows.Next() {
		var id int64
		var shikona sql.NullString
		if err := rows.Scan(&id, &shikona); err != nil {
			continue
		}
		items = append(items, gin.H{
			"id":      strconv.FormatInt(id, 10),
			"shikona": shikona.String,
		})
	}

	c.JSON(http.StatusOK, gin.H{"items": items})
}
