package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

// SearchRikishi performs a MongoDB Atlas Search against the `rikishi_pages`
// collection using the configured search index (defaults to "default").
// Query params:
//   - q: search query (required)
//   - limit: optional integer limit (default 20, capped at 100)
func (a *App) SearchRikishi(c *gin.Context) {
	q := c.Query("q")
	if q == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing query parameter 'q'"})
		return
	}

	limitStr := c.DefaultQuery("limit", "20")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	coll := a.Mongo.Collection("rikishi_pages")

	// Use Atlas Search $search stage against the default index. The index
	// mapping expects the rikishi document under field `rikishi` with
	// subfield `shikona` so we search that path.
	pipeline := bson.A{
		bson.D{{"$search", bson.D{
			{"index", "default"},
			// Use fuzzy text search so minor typos and small edits still match.
			{"text", bson.D{{"query", q}, {"path", "rikishi.shikona"}, {"fuzzy", bson.D{{"maxEdits", 1}, {"maxExpansions", 50}}}}},
		}}},
		// Add score meta so clients can rank if desired
		bson.D{{"$addFields", bson.D{{"score", bson.D{{"$meta", "searchScore"}}}}}},
		bson.D{{"$limit", limit}},
		// Project to return only the rikishi summary + score
		bson.D{{"$project", bson.D{{"_id", 0}, {"rikishi", 1}, {"score", 1}}}},
	}

	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "search failed"})
		return
	}
	defer cur.Close(ctx)

	var results []bson.M
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		results = append(results, doc)
	}
	if err := cur.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "error reading search results"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"items": results, "total": len(results)})
}
