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

// GetBasho returns a basho page document from the `basho_pages` collection
// It looks up the document by the `id` field (string) matching the route param.
func (a *App) GetBasho(c *gin.Context) {
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

	coll := a.Mongo.Collection("basho_pages")
	var doc bson.M
	err = coll.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "basho not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch basho"})
		return
	}

	c.JSON(http.StatusOK, doc)
}

// ListBasho returns a list of basho ids from Postgres (table: basho)
func (a *App) ListBasho(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	rows, err := a.PG.Query(ctx, "SELECT id, location, start_date, end_date FROM basho ORDER BY id")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer rows.Close()

	var items []gin.H
	for rows.Next() {
		var id int64
		var location sql.NullString
		var start sql.NullTime
		var end sql.NullTime
		if err := rows.Scan(&id, &location, &start, &end); err != nil {
			continue
		}
		item := gin.H{"id": strconv.FormatInt(id, 10)}
		if location.Valid {
			item["location"] = location.String
		} else {
			item["location"] = ""
		}
		if start.Valid {
			item["start_date"] = start.Time.Format("2006-01-02")
		} else {
			item["start_date"] = ""
		}
		if end.Valid {
			item["end_date"] = end.Time.Format("2006-01-02")
		} else {
			item["end_date"] = ""
		}
		items = append(items, item)
	}

	c.JSON(http.StatusOK, gin.H{"items": items})
}
