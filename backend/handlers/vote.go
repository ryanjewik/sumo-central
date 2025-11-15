package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

// buildCanonicalFromMap attempts to construct the canonical composite id from
// a Mongo match object (bson.M) using fields: basho_id, day, match_number,
// east_rikishi_id and west_rikishi_id. Returns (id, true) on success.
func buildCanonicalFromMap(m bson.M) (int64, bool) {
	// helper to read possible field names and normalize to string
	toStr := func(v interface{}) string {
		if v == nil {
			return ""
		}
		switch t := v.(type) {
		case int32:
			return strconv.FormatInt(int64(t), 10)
		case int64:
			return strconv.FormatInt(t, 10)
		case float64:
			return strconv.FormatInt(int64(t), 10)
		case string:
			return t
		default:
			return fmt.Sprintf("%v", t)
		}
	}

	basho := toStr(m["basho_id"])
	if basho == "" {
		basho = toStr(m["basho"])
	}
	if basho == "" {
		basho = toStr(m["tournament_id"])
	}
	day := toStr(m["day"])
	if day == "" {
		day = toStr(m["Day"])
	}
	if day == "" {
		day = toStr(m["match_day"])
	}
	matchNo := toStr(m["match_number"])
	if matchNo == "" {
		matchNo = toStr(m["matchNo"])
	}
	if matchNo == "" {
		matchNo = toStr(m["matchNumber"])
	}
	east := toStr(m["east_rikishi_id"])
	if east == "" {
		east = toStr(m["eastId"])
	}
	if east == "" {
		east = toStr(m["east_id"])
	}
	if east == "" {
		east = toStr(m["rikishi2_id"])
	}
	west := toStr(m["west_rikishi_id"])
	if west == "" {
		west = toStr(m["westId"])
	}
	if west == "" {
		west = toStr(m["west_id"])
	}
	if west == "" {
		west = toStr(m["rikishi1_id"])
	}

	if basho == "" || day == "" || matchNo == "" || east == "" || west == "" {
		return 0, false
	}
	idStr := basho + day + matchNo + east + west
	if idStr == "" {
		return 0, false
	}
	if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
		return id, true
	}
	return 0, false
}

// Vote persists or updates a user's prediction for a match.
// Redis is used as the source-of-truth when available via an atomic Lua script.
// If Redis is unavailable, the handler falls back to persisting the prediction
// directly to Postgres to preserve user intent.
func (a *App) Vote(c *gin.Context) {
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

	// Resolve legacy/placeholder short ids to canonical composite ids.
	// If the provided matchID exists in the matches table, use it. Otherwise
	// try to find a matching entry in the homepage/upcoming documents in Mongo
	// and build the canonical id from basho_id + day + match_number + east + west.
	resolvedMatchID := matchID
	// perform resolution with its own short timeout context
	ctxResolve, cancelResolve := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancelResolve()

	// quick DB probe: check if a match with this id exists (canonical)
	var probe int64
	if a.PG != nil {
		errProbe := a.PG.QueryRow(ctxResolve, "SELECT id FROM matches WHERE id=$1", matchID).Scan(&probe)
		if errProbe == nil {
			resolvedMatchID = probe
		}
	}
	if resolvedMatchID == matchID {
		// not found in matches table or no PG available; try Mongo mapping
		if a.Mongo != nil {
			// look for upcoming_matches (or recent_matches) containing this id
			coll := a.Mongo.Collection("homepage")
			// try numeric matchID and string form
			filter := bson.M{"$or": bson.A{
				bson.M{"upcoming_matches.id": matchID},
				bson.M{"upcoming_matches.id": strconv.FormatInt(matchID, 10)},
				bson.M{"recent_matches.id": matchID},
				bson.M{"recent_matches.id": strconv.FormatInt(matchID, 10)},
			}}
			var doc bson.M
			if err := coll.FindOne(ctxResolve, filter).Decode(&doc); err == nil {
				// search arrays for the specific element
				tryFind := func(arrKey string) (int64, bool) {
					if raw, ok := doc[arrKey]; ok {
						switch t := raw.(type) {
						case bson.A:
							for _, it := range t {
								if m, ok := it.(bson.M); ok {
									// match id field might be numeric or string
									if v, ok := m["id"]; ok {
										switch vv := v.(type) {
										case int32:
											if int64(vv) == matchID {
												return buildCanonicalFromMap(m)
											}
										case int64:
											if vv == matchID {
												return buildCanonicalFromMap(m)
											}
										case float64:
											if int64(vv) == matchID {
												return buildCanonicalFromMap(m)
											}
										case string:
											if vv == strconv.FormatInt(matchID, 10) {
												return buildCanonicalFromMap(m)
											}
										}
									}
								}
							}
						case bson.M:
							for _, v := range t {
								if m, ok := v.(bson.M); ok {
									if v2, ok := m["id"]; ok {
										switch vv := v2.(type) {
										case int32:
											if int64(vv) == matchID {
												return buildCanonicalFromMap(m)
											}
										case int64:
											if vv == matchID {
												return buildCanonicalFromMap(m)
											}
										case float64:
											if int64(vv) == matchID {
												return buildCanonicalFromMap(m)
											}
										case string:
											if vv == strconv.FormatInt(matchID, 10) {
												return buildCanonicalFromMap(m)
											}
										}
									}
								}
							}
						}
					}
					return 0, false
				}
				if v, ok := tryFind("upcoming_matches"); ok {
					resolvedMatchID = v
				} else if v, ok := tryFind("recent_matches"); ok {
					resolvedMatchID = v
				}
			}
		}
	}
	// use resolvedMatchID from now on
	matchID = resolvedMatchID

	var body struct {
		Winner int64 `json:"winner"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Redis-first path
	if a.Redis != nil {
		votesKey := fmt.Sprintf("match_votes:%d", matchID)
		usersKey := fmt.Sprintf("match_users:%d", matchID)
		newField := strconv.FormatInt(body.Winner, 10)

		lua := `
local votes_key = KEYS[1]
local users_key = KEYS[2]
local user = ARGV[1]
local new = ARGV[2]
local match = ARGV[3]
local prev = redis.call('HGET', users_key, user)
if prev == new then
  -- no-op
else
  if prev then redis.call('HINCRBY', votes_key, prev, -1) end
  redis.call('HINCRBY', votes_key, new, 1)
  redis.call('HSET', users_key, user, new)
end
local flat = redis.call('HGETALL', votes_key)
local t = {}
for i=1,#flat,2 do t[flat[i]] = flat[i+1] end
local payload = {match=match, counts=t, user=user, new=new}
local encoded = cjson.encode(payload)
redis.call('PUBLISH', 'match_updates:'..match, encoded)
return encoded
`

		res, err := a.Redis.Eval(ctx, lua, []string{votesKey, usersKey}, userID, newField, fmt.Sprintf("%d", matchID)).Result()
		if err == nil {
			if s, ok := res.(string); ok {
				var out map[string]any
				if err := json.Unmarshal([]byte(s), &out); err == nil {
					// Persist the user's prediction to Postgres asynchronously so we have
					// durable storage even when Redis is the primary source-of-truth.
					// This is best-effort and runs in the background to avoid adding
					// latency to the user-facing request.
					if a.PG != nil {
						// Attempt to persist synchronously so failures are visible in logs.
						ctxBg, cancelBg := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancelBg()
						if _, err := a.PG.Exec(ctxBg, `
							INSERT INTO match_predictions (user_id, match_id, predicted_winner, prediction_time)
							VALUES ($1, $2, $3, NOW())
							ON CONFLICT (user_id, match_id) DO UPDATE
							  SET predicted_winner = EXCLUDED.predicted_winner,
								  prediction_time = NOW()
						`, userID, matchID, body.Winner); err != nil {
							// Log the error so we can diagnose why persistence may be failing
							fmt.Printf("vote: failed to persist prediction user=%s match=%d winner=%d err=%v\n", userID, matchID, body.Winner, err)
						}
					}
					c.JSON(http.StatusOK, gin.H{"result": out})
					return
				}
			}
		} else {
			// log the error on the context and fall through to DB fallback
			c.Error(err)
		}
	}

	// Fallback: persist to Postgres directly
	_, err = a.PG.Exec(ctx, `
		INSERT INTO match_predictions (user_id, match_id, predicted_winner, prediction_time)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (user_id, match_id) DO UPDATE
		  SET predicted_winner = EXCLUDED.predicted_winner,
			  prediction_time = NOW()
	`, userID, matchID, body.Winner)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save prediction"})
		return
	}

	// After persisting to Postgres, compute aggregate counts and publish an update
	// so connected clients still receive live updates even when Redis path failed earlier.
	counts := map[string]int64{}
	rows, err := a.PG.Query(ctx, `SELECT predicted_winner, COUNT(*) FROM match_predictions WHERE match_id=$1 GROUP BY predicted_winner`, matchID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var winner sql.NullInt64
			var cnt int64
			if err := rows.Scan(&winner, &cnt); err == nil && winner.Valid {
				counts[strconv.FormatInt(winner.Int64, 10)] = cnt
			}
		}
	}

	// Try to publish to Redis if available so subscribers get the update.
	if a.Redis != nil {
		votesKey := fmt.Sprintf("match_votes:%d", matchID)
		// update Redis hash with aggregated counts (best-effort)
		if len(counts) > 0 {
			m := make(map[string]interface{}, len(counts))
			for k, v := range counts {
				m[k] = v
			}
			_ = a.Redis.HSet(ctx, votesKey, m).Err()
		}
		// publish a payload to the channel
		payload := map[string]any{"match": fmt.Sprintf("%d", matchID), "counts": counts, "user": userID, "new": strconv.FormatInt(body.Winner, 10)}
		if b, err := json.Marshal(payload); err == nil {
			_ = a.Redis.Publish(ctx, "match_updates:"+fmt.Sprintf("%d", matchID), string(b)).Err()
		}
	}

	// return counts to the client so the UI can update immediately
	c.JSON(http.StatusOK, gin.H{"result": map[string]any{"counts": counts, "user": userID, "new": strconv.FormatInt(body.Winner, 10)}})
}

// GetMatchVotes returns the current vote counts for a match (from Redis when available).
func (a *App) GetMatchVotes(c *gin.Context) {
	matchIDStr := c.Param("id")
	matchID, err := strconv.ParseInt(matchIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid match id"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("match_votes:%d", matchID)
	res, err := a.Redis.HGetAll(ctx, key).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read votes"})
		return
	}
	out := map[string]int64{}
	for k, v := range res {
		n, _ := strconv.ParseInt(v, 10, 64)
		out[k] = n
	}
	// If the requester is authenticated, also return their current prediction (if any)
	userVote := interface{}(nil)
	if ui, ok := c.Get("user_id"); ok {
		if userID, ok := ui.(string); ok && userID != "" {
			usersKey := fmt.Sprintf("match_users:%d", matchID)
			// Try Redis first
			if a.Redis != nil {
				if uv, err := a.Redis.HGet(ctx, usersKey, userID).Result(); err == nil {
					if uv != "" {
						userVote = uv
					}
				}
			}
			// Fallback to Postgres if Redis didn't have it
			if userVote == nil && a.PG != nil {
				var pred sql.NullInt64
				err := a.PG.QueryRow(ctx, `SELECT predicted_winner FROM match_predictions WHERE user_id=$1 AND match_id=$2`, userID, matchID).Scan(&pred)
				if err == nil && pred.Valid {
					userVote = strconv.FormatInt(pred.Int64, 10)
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"counts": out, "user_vote": userVote})
}
