package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Match struct {
	ID       string `json:"id" bson:"_id,omitempty"`
	BashoID  string `json:"bashoId" bson:"bashoId"`
	RikishiA string `json:"rikishiA" bson:"rikishiA"`
	RikishiB string `json:"rikishiB" bson:"rikishiB"`
	Winner   string `json:"winner" bson:"winner"`
}

func (a *App) GetMatches(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	coll := a.Mongo.Collection("matches")
	cur, err := coll.Find(ctx, struct{}{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer cur.Close(ctx)

	var matches []Match
	if err := cur.All(ctx, &matches); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "decode error"})
		return
	}

	c.JSON(http.StatusOK, matches)
}
