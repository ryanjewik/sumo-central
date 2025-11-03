package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Home returns the homepage document from MongoDB in the `homepage` collection
// where the `_homepage_doc` key is true. If no document is found a 404 is
// returned. On other errors a 500 is returned.
func (a *App) Home(c *gin.Context) {
	// short timeout for DB operations
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	coll := a.Mongo.Collection("homepage")
	var doc bson.M
	err := coll.FindOne(ctx, bson.M{"_homepage_doc": true}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "homepage document not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load homepage"})
		return
	}

	c.JSON(http.StatusOK, doc)
}
