package handlers

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CreateForum creates a new forum post in the `forums` collection. Expected
// JSON body: { title: string, body: string, tags?: []string }
// If authenticated, middleware may have set "user_id" and "username" in the gin.Context.
func (a *App) CreateForum(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	var in struct {
		Title string   `json:"title" binding:"required"`
		Body  string   `json:"body" binding:"required"`
		Tags  []string `json:"tags"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	authorID := ""
	authorUsername := "anonymous"
	if v, ok := c.Get("user_id"); ok {
		authorID = stringify(v)
	}
	if v, ok := c.Get("username"); ok {
		if s, ok2 := v.(string); ok2 && s != "" {
			authorUsername = s
		}
	}

	coll := a.Mongo.Collection("discussions")
	now := time.Now().UTC()
	doc := bson.M{
		"title":           in.Title,
		"body":            in.Body,
		"tags":            in.Tags,
		"author_username": authorUsername,
		"author_id":       authorID,
		"created_at":      now,
		"upvotes":         0,
		"downvotes":       0,
	}

	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to insert"})
		return
	}

	// Build a response object with id and created_at formatted
	idHex := ""
	if oid, ok := res.InsertedID.(primitive.ObjectID); ok {
		idHex = oid.Hex()
	}
	doc["id"] = idHex
	// created_at as RFC3339 string for frontend convenience
	doc["created_at"] = now.Format(time.RFC3339)
	delete(doc, "_id")

	// If the discussion had an authenticated author_id, increment that user's
	// num_posts counter in Postgres. This is best-effort: if the Postgres
	// update fails we log a warning but still return success for the created
	// discussion so the forum UX is not blocked by a transient DB issue.
	if authorID != "" {
		if _, err := a.PG.Exec(ctx, "UPDATE users SET num_posts = COALESCE(num_posts,0) + 1 WHERE id = $1", authorID); err != nil {
			log.Printf("warning: failed to increment num_posts for user %s: %v", authorID, err)
		}
	}

	c.JSON(http.StatusOK, doc)
}

// ListForums returns forum posts paginated by skip/limit query parameters. By
// default limit=20. Sorts by created_at desc.
func (a *App) ListForums(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	qlimit := 20
	if s := c.Query("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 100 {
			qlimit = n
		}
	}
	qskip := int64(0)
	if s := c.Query("skip"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= 0 {
			qskip = n
		}
	}

	coll := a.Mongo.Collection("discussions")
	opts := options.Find()
	opts.SetSort(bson.D{{Key: "created_at", Value: -1}})
	opts.SetSkip(qskip)
	opts.SetLimit(int64(qlimit))

	// Support optional filtering by author_id so the frontend can show a
	// user's authored posts on their profile page. If author_id is provided
	// we'll filter the Mongo query accordingly.
	filter := bson.M{}
	if author := c.Query("author_id"); author != "" {
		filter["author_id"] = author
	}

	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer cur.Close(ctx)

	var out []bson.M
	if err := cur.All(ctx, &out); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db read error"})
		return
	}

	// Normalize _id -> id (hex) and created_at formatting
	for i := range out {
		if v, ok := out[i]["_id"].(primitive.ObjectID); ok {
			out[i]["id"] = v.Hex()
		}
		if t, ok := out[i]["created_at"].(primitive.DateTime); ok {
			out[i]["created_at"] = t.Time().Format(time.RFC3339)
		} else if tt, ok := out[i]["created_at"].(time.Time); ok {
			out[i]["created_at"] = tt.Format(time.RFC3339)
		}
		delete(out[i], "_id")
	}

	c.JSON(http.StatusOK, out)
}

// GetDiscussion returns a single discussion post by its ObjectID hex (path param :id)
func (a *App) GetDiscussion(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id"})
		return
	}

	coll := a.Mongo.Collection("discussions")

	// Try to parse as ObjectID first
	var filter bson.M
	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		filter = bson.M{"_id": oid}
	} else {
		// fallback: attempt to match id field if present
		filter = bson.M{"id": id}
	}

	var out bson.M
	if err := coll.FindOne(ctx, filter).Decode(&out); err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "discussion not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	if v, ok := out["_id"].(primitive.ObjectID); ok {
		out["id"] = v.Hex()
	}
	if t, ok := out["created_at"].(primitive.DateTime); ok {
		out["created_at"] = t.Time().Format(time.RFC3339)
	} else if tt, ok := out["created_at"].(time.Time); ok {
		out["created_at"] = tt.Format(time.RFC3339)
	}
	delete(out, "_id")

	c.JSON(http.StatusOK, out)
}

// AddComment appends a comment (or reply) to the discussion document's comments array.
// Requires authentication middleware so author info can be set from context.
func (a *App) AddComment(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id"})
		return
	}

	var in struct {
		Body     string `json:"body" binding:"required"`
		ParentID string `json:"parent_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	authorID := ""
	authorUsername := "anonymous"
	if v, ok := c.Get("user_id"); ok {
		authorID = stringify(v)
	}
	if v, ok := c.Get("username"); ok {
		if s, ok2 := v.(string); ok2 && s != "" {
			authorUsername = s
		}
	}

	coll := a.Mongo.Collection("discussions")
	now := time.Now().UTC()

	commentID := primitive.NewObjectID()
	comment := bson.M{
		"_id":             commentID,
		"id":              commentID.Hex(),
		"parent_id":       in.ParentID,
		"body":            in.Body,
		"author_username": authorUsername,
		"author_id":       authorID,
		"created_at":      now,
		"upvotes":         0,
		"downvotes":       0,
	}

	// push to comments array
	filter := bson.M{}
	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		filter = bson.M{"_id": oid}
	} else {
		filter = bson.M{"id": id}
	}

	upd := bson.M{"$push": bson.M{"comments": comment}}
	if _, err := coll.UpdateOne(ctx, filter, upd); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to append comment"})
		return
	}

	// respond with created comment (format created_at)
	comment["created_at"] = now.Format(time.RFC3339)
	c.JSON(http.StatusOK, comment)
}

// VoteDiscussion increments upvotes or downvotes on a discussion document.
func (a *App) VoteDiscussion(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id"})
		return
	}

	var in struct {
		Direction string `json:"direction" binding:"required"` // "up" or "down"
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// require authenticated user
	userID := ""
	if v, ok := c.Get("user_id"); ok {
		userID = stringify(v)
	}
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthenticated"})
		return
	}

	// normalize desired value
	var newVal int
	if in.Direction == "up" {
		newVal = 1
	} else if in.Direction == "down" {
		newVal = -1
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid direction"})
		return
	}

	votesColl := a.Mongo.Collection("votes")
	voteFilter := bson.M{"target": "discussion", "discussion_id": id, "user_id": userID}

	var existing bson.M
	err := votesColl.FindOne(ctx, voteFilter).Decode(&existing)
	coll := a.Mongo.Collection("discussions")

	now := time.Now().UTC()
	if err == mongo.ErrNoDocuments {
		// create vote record and increment counts
		vdoc := bson.M{"target": "discussion", "discussion_id": id, "user_id": userID, "value": newVal, "created_at": now}
		if _, err = votesColl.InsertOne(ctx, vdoc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record vote"})
			return
		}
		// increment discussion counts
		var inc bson.M
		if newVal == 1 {
			inc = bson.M{"$inc": bson.M{"upvotes": 1}}
		} else {
			inc = bson.M{"$inc": bson.M{"downvotes": 1}}
		}
		filter := bson.M{}
		if oid, err := primitive.ObjectIDFromHex(id); err == nil {
			filter = bson.M{"_id": oid}
		} else {
			filter = bson.M{"id": id}
		}
		if _, err = coll.UpdateOne(ctx, filter, inc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update vote counts"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	// existing vote found
	var existingVal int
	switch t := existing["value"].(type) {
	case int32:
		existingVal = int(t)
	case int64:
		existingVal = int(t)
	case float64:
		existingVal = int(t)
	case int:
		existingVal = t
	default:
		existingVal = 0
	}

	if existingVal == newVal {
		c.JSON(http.StatusBadRequest, gin.H{"error": "already voted"})
		return
	}

	// switch vote: update vote doc and adjust counts
	update := bson.M{"$set": bson.M{"value": newVal, "updated_at": now}}
	if _, err = votesColl.UpdateOne(ctx, voteFilter, update); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update vote record"})
		return
	}

	// compute counts delta
	var inc bson.M
	if existingVal == 1 && newVal == -1 {
		inc = bson.M{"$inc": bson.M{"upvotes": -1, "downvotes": 1}}
	} else if existingVal == -1 && newVal == 1 {
		inc = bson.M{"$inc": bson.M{"upvotes": 1, "downvotes": -1}}
	} else {
		// fallback: no-op
		inc = bson.M{}
	}

	filter := bson.M{}
	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		filter = bson.M{"_id": oid}
	} else {
		filter = bson.M{"id": id}
	}
	if len(inc) > 0 {
		if _, err = coll.UpdateOne(ctx, filter, inc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update vote counts"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// VoteComment increments upvotes/downvotes for a specific comment inside the discussion document.
func (a *App) VoteComment(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	id := c.Param("id")
	commentId := c.Param("commentId")
	if id == "" || commentId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id or commentId"})
		return
	}

	var in struct {
		Direction string `json:"direction" binding:"required"` // "up" or "down"
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// convert ids
	var filter bson.M
	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		filter = bson.M{"_id": oid, "comments._id": primitive.NilObjectID}
		// we'll set comments._id filter below when parsing commentId
	} else {
		filter = bson.M{"id": id, "comments._id": primitive.NilObjectID}
	}

	commentOid, err := primitive.ObjectIDFromHex(commentId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid commentId"})
		return
	}
	// set comment filter
	filter["comments._id"] = commentOid

	// require authenticated user
	userID := ""
	if v, ok := c.Get("user_id"); ok {
		userID = stringify(v)
	}
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthenticated"})
		return
	}

	// validate direction
	var newVal int
	if in.Direction == "up" {
		newVal = 1
	} else if in.Direction == "down" {
		newVal = -1
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid direction"})
		return
	}

	votesColl := a.Mongo.Collection("votes")
	voteFilter := bson.M{"target": "comment", "discussion_id": id, "comment_id": commentId, "user_id": userID}
	var existing bson.M
	err = votesColl.FindOne(ctx, voteFilter).Decode(&existing)
	coll := a.Mongo.Collection("discussions")

	now := time.Now().UTC()
	if err == mongo.ErrNoDocuments {
		// insert vote record
		vdoc := bson.M{"target": "comment", "discussion_id": id, "comment_id": commentId, "user_id": userID, "value": newVal, "created_at": now}
		if _, err = votesColl.InsertOne(ctx, vdoc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record vote"})
			return
		}
		// increment comment counts
		var inc bson.M
		if newVal == 1 {
			inc = bson.M{"$inc": bson.M{"comments.$.upvotes": 1}}
		} else {
			inc = bson.M{"$inc": bson.M{"comments.$.downvotes": 1}}
		}
		if _, err = coll.UpdateOne(ctx, filter, inc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update comment vote"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	// existing vote found
	var existingVal int
	switch t := existing["value"].(type) {
	case int32:
		existingVal = int(t)
	case int64:
		existingVal = int(t)
	case float64:
		existingVal = int(t)
	case int:
		existingVal = t
	default:
		existingVal = 0
	}

	if existingVal == newVal {
		c.JSON(http.StatusBadRequest, gin.H{"error": "already voted"})
		return
	}

	// update vote doc
	if _, err = votesColl.UpdateOne(ctx, voteFilter, bson.M{"$set": bson.M{"value": newVal, "updated_at": now}}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update vote record"})
		return
	}

	// adjust comment counts
	var inc bson.M
	if existingVal == 1 && newVal == -1 {
		inc = bson.M{"$inc": bson.M{"comments.$.upvotes": -1, "comments.$.downvotes": 1}}
	} else if existingVal == -1 && newVal == 1 {
		inc = bson.M{"$inc": bson.M{"comments.$.upvotes": 1, "comments.$.downvotes": -1}}
	} else {
		inc = bson.M{}
	}
	if len(inc) > 0 {
		if _, err = coll.UpdateOne(ctx, filter, inc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update comment vote"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// stringify converts interface values commonly stored in gin context to string
func stringify(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}
