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

	// Use the raw path param string for keying Redis so GET and POST agree.
	matchIDStr := c.Param("id")

	// Try to parse numeric form when we need to query numeric DB fields. If
	// parsing fails, we continue and attempt to resolve using Mongo where
	// possible. Do not reject on non-numeric here because large composite ids
	// may be passed as strings by clients and should be handled.
	var matchID int64
	var parseErr error
	if matchIDStr != "" {
		matchID, parseErr = strconv.ParseInt(matchIDStr, 10, 64)
	}

	// Resolve legacy/placeholder short ids to canonical composite ids.
	// If the provided numeric parse succeeded and the id exists in the
	// matches table, use it. Otherwise try to find a matching entry in the
	// homepage/upcoming documents in Mongo and build the canonical id from
	// basho_id + day + match_number + east + west. We keep both the numeric
	// resolved id (when available) and the canonical string to use for Redis
	// keys and pubsub so reads/writes are consistent.
	resolvedMatchID := int64(0)
	// prefer numeric probe when parse succeeded
	if parseErr == nil {
		resolvedMatchID = matchID
	}
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
	// use resolvedMatchID when available; we'll also keep a canonical string
	// to consistently key Redis and publish channels.
	if resolvedMatchID != 0 {
		matchID = resolvedMatchID
	}
	canonicalMatchIDStr := matchIDStr
	if resolvedMatchID != 0 {
		canonicalMatchIDStr = strconv.FormatInt(resolvedMatchID, 10)
	}

	// Accept winner as number or string in JSON; be permissive to client
	// variations (some client code may send winner as string).
	var rawBody map[string]interface{}
	if err := c.ShouldBindJSON(&rawBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	var winnerVal int64
	if w, ok := rawBody["winner"]; ok {
		switch t := w.(type) {
		case float64:
			winnerVal = int64(t)
		case int:
			winnerVal = int64(t)
		case int64:
			winnerVal = t
		case string:
			if t == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid winner"})
				return
			}
			if parsed, err := strconv.ParseInt(t, 10, 64); err == nil {
				winnerVal = parsed
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid winner"})
				return
			}
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid winner"})
			return
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing winner"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// Redis-first path
	if a.Redis != nil {
		// Use the canonical string form for keys so GET and POST use the same
		// Redis keys regardless of whether the incoming id was numeric or
		// resolved from Mongo. This avoids mismatches where writes go to
		// numeric keys and reads look at string keys (or vice-versa).
		votesKey := fmt.Sprintf("match_votes:%s", canonicalMatchIDStr)
		usersKey := fmt.Sprintf("match_users:%s", canonicalMatchIDStr)
		newField := strconv.FormatInt(winnerVal, 10)

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

		// Pass canonicalMatchIDStr as the match identifier to the Lua script
		// and publish channel so subscribers receive the same string id.
		res, err := a.Redis.Eval(ctx, lua, []string{votesKey, usersKey}, userID, newField, canonicalMatchIDStr).Result()
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
						// Persist to Postgres using numeric id when available. If we
						// don't have a numeric (resolved) id, attempt to parse the
						// canonical string; if that fails the DB insert cannot
						// complete because match_predictions.match_id is BIGINT.
						var pgMatchID int64
						if resolvedMatchID != 0 {
							pgMatchID = resolvedMatchID
						} else {
							if parsed, err := strconv.ParseInt(canonicalMatchIDStr, 10, 64); err == nil {
								pgMatchID = parsed
							} else {
								// give up persisting to PG (but Redis was updated)
								pgMatchID = 0
							}
						}
						if pgMatchID != 0 {
							if _, err := a.PG.Exec(ctxBg, `
								INSERT INTO match_predictions (user_id, match_id, predicted_winner, prediction_time)
								VALUES ($1, $2, $3, NOW())
								ON CONFLICT (user_id, match_id) DO UPDATE
								  SET predicted_winner = EXCLUDED.predicted_winner,
									  prediction_time = NOW()
							`, userID, pgMatchID, winnerVal); err != nil {
								// Log the error so we can diagnose why persistence may be failing
								fmt.Printf("vote: failed to persist prediction user=%s match=%v winner=%d err=%v\n", userID, pgMatchID, winnerVal, err)
							}
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
	// Fallback DB persistence (when Redis not available or Eval failed)
	var dbMatchID int64
	if resolvedMatchID != 0 {
		dbMatchID = resolvedMatchID
	} else {
		if parsed, err := strconv.ParseInt(canonicalMatchIDStr, 10, 64); err == nil {
			dbMatchID = parsed
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid match id for DB persistence"})
			return
		}
	}

	_, err = a.PG.Exec(ctx, `
		INSERT INTO match_predictions (user_id, match_id, predicted_winner, prediction_time)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (user_id, match_id) DO UPDATE
		  SET predicted_winner = EXCLUDED.predicted_winner,
			  prediction_time = NOW()
	`, userID, dbMatchID, winnerVal)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save prediction"})
		return
	}

	// After persisting to Postgres, compute aggregate counts and publish an update
	// so connected clients still receive live updates even when Redis path failed earlier.
	counts := map[string]int64{}
	rows, err := a.PG.Query(ctx, `SELECT predicted_winner, COUNT(*) FROM match_predictions WHERE match_id=$1 GROUP BY predicted_winner`, dbMatchID)
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
		votesKey := fmt.Sprintf("match_votes:%s", canonicalMatchIDStr)
		// update Redis hash with aggregated counts (best-effort)
		if len(counts) > 0 {
			m := make(map[string]interface{}, len(counts))
			for k, v := range counts {
				m[k] = v
			}
			_ = a.Redis.HSet(ctx, votesKey, m).Err()
		}
		// publish a payload to the channel
		payload := map[string]any{"match": canonicalMatchIDStr, "counts": counts, "user": userID, "new": strconv.FormatInt(winnerVal, 10)}
		if b, err := json.Marshal(payload); err == nil {
			_ = a.Redis.Publish(ctx, "match_updates:"+canonicalMatchIDStr, string(b)).Err()
		}
	}

	// return counts to the client so the UI can update immediately
	c.JSON(http.StatusOK, gin.H{"result": map[string]any{"counts": counts, "user": userID, "new": strconv.FormatInt(winnerVal, 10)}})
}

// GetMatchVotes returns the current vote counts for a match (from Redis when available).
func (a *App) GetMatchVotes(c *gin.Context) {
	matchIDStr := c.Param("id")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	// Attempt to read live counts from Redis when available. If Redis is
	// unavailable or the key is empty (deleted by a cleanup job), fall back to
	// computing aggregates from Postgres so the UI can still render vote bars.
	out := map[string]int64{}
	usedRedis := false

	if a.Redis != nil {
		// Use the raw path param as the key suffix so string or numeric ids work
		key := fmt.Sprintf("match_votes:%s", matchIDStr)
		res, err := a.Redis.HGetAll(ctx, key).Result()
		if err == nil {
			if len(res) > 0 {
				for k, v := range res {
					n, _ := strconv.ParseInt(v, 10, 64)
					out[k] = n
				}
				usedRedis = true
			}
		} else {
			// If Redis call failed, log the error on the Gin context and fall
			// through to Postgres fallback below.
			c.Error(err)
		}
	}

	// If Redis wasn't used (disabled, error, or empty key), fallback to Postgres
	// aggregates so recent_matches_list can still show vote bars.
	if !usedRedis {
		if a.PG == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no data source available for votes"})
			return
		}
		// Use a text comparison on match_id so both numeric and string match ids
		// stored in Postgres are handled (casts integer ids to text when needed).
		rows, err := a.PG.Query(ctx, `SELECT predicted_winner, COUNT(*) FROM match_predictions WHERE CAST(match_id AS text) = $1 GROUP BY predicted_winner`, matchIDStr)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read votes"})
			return
		}
		defer rows.Close()
		for rows.Next() {
			var winner sql.NullInt64
			var cnt int64
			if err := rows.Scan(&winner, &cnt); err == nil && winner.Valid {
				out[strconv.FormatInt(winner.Int64, 10)] = cnt
			}
		}
	}

	// If the requester is authenticated, also return their current prediction (if any)
	userVote := interface{}(nil)
	if ui, ok := c.Get("user_id"); ok {
		if userID, ok := ui.(string); ok && userID != "" {
			// Try Redis first for user vote (fast). If Redis isn't available or
			// didn't have a value, fall back to Postgres.
			if a.Redis != nil {
				usersKey := fmt.Sprintf("match_users:%s", matchIDStr)
				if uv, err := a.Redis.HGet(ctx, usersKey, userID).Result(); err == nil {
					if uv != "" {
						userVote = uv
					}
				}
			}
			if userVote == nil && a.PG != nil {
				var pred sql.NullInt64
				err := a.PG.QueryRow(ctx, `SELECT predicted_winner FROM match_predictions WHERE user_id=$1 AND CAST(match_id AS text) = $2`, userID, matchIDStr).Scan(&pred)
				if err == nil && pred.Valid {
					userVote = strconv.FormatInt(pred.Int64, 10)
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"counts": out, "user_vote": userVote})
}
