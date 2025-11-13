package handlers

import (
	"context"
	"net/http"
	"strconv"
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

	// Enrich upcoming_matches and recent_matches with rikishi summary fields
	// so the frontend can render images, ranks and basic stats without extra calls.
	tryEnrich := func() {
		// helper to extract numeric id from various possible fields
		getNumericID := func(m bson.M, keys ...string) (int64, bool) {
			for _, k := range keys {
				if v, ok := m[k]; ok {
					switch t := v.(type) {
					case int32:
						return int64(t), true
					case int64:
						return t, true
					case float64:
						return int64(t), true
					case string:
						if n, err := strconv.ParseInt(t, 10, 64); err == nil {
							return n, true
						}
					}
				}
			}
			return 0, false
		}

		enrichMatch := func(m bson.M) {
			// possible rikishi id fields
			var westId, eastId int64
			if id, ok := getNumericID(m, "west_rikishi_id", "west_id", "westId", "rikishi1_id"); ok {
				westId = id
			}
			if id, ok := getNumericID(m, "east_rikishi_id", "east_id", "eastId", "rikishi2_id"); ok {
				eastId = id
			}
			// If the match already contains nested rikishi objects (enriched by
			// the background jobs), prefer those and avoid extra DB reads.
			if wrk, ok := m["west_rikishi"].(bson.M); ok {
				if s, ok := wrk["s3_url"].(string); ok && s != "" {
					m["west_image"] = s
				} else if s, ok := wrk["pfp_url"].(string); ok && s != "" {
					m["west_image"] = s
				} else if s, ok := wrk["image_url"].(string); ok && s != "" {
					m["west_image"] = s
				}
				if rv, ok := wrk["current_rank"]; ok {
					if rv == nil {
						m["west_rank"] = "NA"
					} else {
						m["west_rank"] = rv
					}
				}
				if h, ok := wrk["current_height"]; ok {
					m["west_current_height"] = h
				}
				if wv, ok := wrk["current_weight"]; ok {
					m["west_current_weight"] = wv
				}
				if wins, ok := wrk["wins"]; ok {
					m["west_wins"] = wins
				}
				if losses, ok := wrk["losses"]; ok {
					m["west_losses"] = losses
				}
				if y, ok := wrk["yusho_count"]; ok {
					m["west_yusho"] = y
				}
				if s, ok := wrk["sansho_count"]; ok {
					m["west_sansho"] = s
				}
				if bd, ok := wrk["birthdate"].(string); ok && bd != "" {
					m["west_birthdate"] = bd
				}
				if heya, ok := wrk["heya"].(string); ok && heya != "" {
					m["west_heya"] = heya
				}
			} else {
				// only query DB when nested object not present
				coll := a.Mongo.Collection("rikishi_pages")
				if westId != 0 {
					filter := bson.M{"$or": bson.A{bson.M{"id": westId}, bson.M{"id": int32(westId)}}}
					var rdoc bson.M
					// use short ctx so these lookups can't hang forever
					if err := coll.FindOne(ctx, filter).Decode(&rdoc); err == nil {
						rk := rdoc["rikishi"]
						if rkMap, ok := rk.(bson.M); ok {
							if s, ok := rkMap["s3_url"].(string); ok && s != "" {
								m["west_image"] = s
							} else if s, ok := rkMap["pfp_url"].(string); ok && s != "" {
								m["west_image"] = s
							} else if s, ok := rkMap["image_url"].(string); ok && s != "" {
								m["west_image"] = s
							}
							if rv, ok := rkMap["current_rank"]; ok {
								if rv == nil {
									m["west_rank"] = "NA"
								} else {
									m["west_rank"] = rv
								}
							}
							if h, ok := rkMap["current_height"]; ok {
								m["west_current_height"] = h
							}
							if w, ok := rkMap["current_weight"]; ok {
								m["west_current_weight"] = w
							}
							if wins, ok := rkMap["wins"]; ok {
								m["west_wins"] = wins
							}
							if losses, ok := rkMap["losses"]; ok {
								m["west_losses"] = losses
							}
							if y, ok := rkMap["yusho_count"]; ok {
								m["west_yusho"] = y
							}
							if s, ok := rkMap["sansho_count"]; ok {
								m["west_sansho"] = s
							}
							if bd, ok := rkMap["birthdate"].(string); ok && bd != "" {
								m["west_birthdate"] = bd
							}
							if heya, ok := rkMap["heya"].(string); ok && heya != "" {
								m["west_heya"] = heya
							}
						}
					}
				}
			}
			if erk, ok := m["east_rikishi"].(bson.M); ok {
				if s, ok := erk["s3_url"].(string); ok && s != "" {
					m["east_image"] = s
				} else if s, ok := erk["pfp_url"].(string); ok && s != "" {
					m["east_image"] = s
				} else if s, ok := erk["image_url"].(string); ok && s != "" {
					m["east_image"] = s
				}
				if rv, ok := erk["current_rank"]; ok {
					if rv == nil {
						m["east_rank"] = "NA"
					} else {
						m["east_rank"] = rv
					}
				}
				if h, ok := erk["current_height"]; ok {
					m["east_current_height"] = h
				}
				if wv, ok := erk["current_weight"]; ok {
					m["east_current_weight"] = wv
				}
				if wins, ok := erk["wins"]; ok {
					m["east_wins"] = wins
				}
				if losses, ok := erk["losses"]; ok {
					m["east_losses"] = losses
				}
				if y, ok := erk["yusho_count"]; ok {
					m["east_yusho"] = y
				}
				if s, ok := erk["sansho_count"]; ok {
					m["east_sansho"] = s
				}
				if bd, ok := erk["birthdate"].(string); ok && bd != "" {
					m["east_birthdate"] = bd
				}
				if heya, ok := erk["heya"].(string); ok && heya != "" {
					m["east_heya"] = heya
				}
			} else {
				coll := a.Mongo.Collection("rikishi_pages")
				if eastId != 0 {
					filter := bson.M{"$or": bson.A{bson.M{"id": eastId}, bson.M{"id": int32(eastId)}}}
					var rdoc bson.M
					if err := coll.FindOne(ctx, filter).Decode(&rdoc); err == nil {
						rk := rdoc["rikishi"]
						if rkMap, ok := rk.(bson.M); ok {
							if s, ok := rkMap["s3_url"].(string); ok && s != "" {
								m["east_image"] = s
							} else if s, ok := rkMap["pfp_url"].(string); ok && s != "" {
								m["east_image"] = s
							} else if s, ok := rkMap["image_url"].(string); ok && s != "" {
								m["east_image"] = s
							}
							if rv, ok := rkMap["current_rank"]; ok {
								if rv == nil {
									m["east_rank"] = "NA"
								} else {
									m["east_rank"] = rv
								}
							}
							if h, ok := rkMap["current_height"]; ok {
								m["east_current_height"] = h
							}
							if w, ok := rkMap["current_weight"]; ok {
								m["east_current_weight"] = w
							}
							if wins, ok := rkMap["wins"]; ok {
								m["east_wins"] = wins
							}
							if losses, ok := rkMap["losses"]; ok {
								m["east_losses"] = losses
							}
							if y, ok := rkMap["yusho_count"]; ok {
								m["east_yusho"] = y
							}
							if s, ok := rkMap["sansho_count"]; ok {
								m["east_sansho"] = s
							}
							if bd, ok := rkMap["birthdate"].(string); ok && bd != "" {
								m["east_birthdate"] = bd
							}
							if heya, ok := rkMap["heya"].(string); ok && heya != "" {
								m["east_heya"] = heya
							}
						}
					}
				}
			}
		}

		// enrich upcoming_matches
		if raw, ok := doc["upcoming_matches"].(bson.A); ok && len(raw) > 0 {
			for i := range raw {
				if m, ok := raw[i].(bson.M); ok {
					enrichMatch(m)
					// write back
					raw[i] = m
				}
			}
			doc["upcoming_matches"] = raw
		}

		// enrich recent_matches if present (could be map or array)
		if rm, ok := doc["recent_matches"]; ok {
			switch t := rm.(type) {
			case bson.A:
				for i := range t {
					if m, ok := t[i].(bson.M); ok {
						enrichMatch(m)
						t[i] = m
					}
				}
				doc["recent_matches"] = t
			case bson.M:
				// values are possibly match objects
				for k, v := range t {
					if m, ok := v.(bson.M); ok {
						enrichMatch(m)
						t[k] = m
					}
				}
				doc["recent_matches"] = t
			}
		}
	}

	// attempt enrichment but do not fail the request if enrichment has issues
	func() {
		defer func() { _ = recover() }()
		tryEnrich()
	}()

	c.JSON(http.StatusOK, doc)
}

// Upcoming returns the dedicated upcoming document (where `upcoming: true`).
// This is a small API convenience so frontends can request the canonical
// upcoming_matches / upcoming_highlighted_match written by background jobs.
func (a *App) Upcoming(c *gin.Context) {
	// short timeout for DB operations
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	coll := a.Mongo.Collection("homepage")
	var doc bson.M
	err := coll.FindOne(ctx, bson.M{"upcoming": true}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "upcoming document not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load upcoming document"})
		return
	}

	// Reuse the same enrichment logic used by the homepage handler so matches
	// include rikishi summary fields for the frontend without extra requests.
	tryEnrich := func() {
		// helper to extract numeric id from various possible fields
		getNumericID := func(m bson.M, keys ...string) (int64, bool) {
			for _, k := range keys {
				if v, ok := m[k]; ok {
					switch t := v.(type) {
					case int32:
						return int64(t), true
					case int64:
						return t, true
					case float64:
						return int64(t), true
					case string:
						if n, err := strconv.ParseInt(t, 10, 64); err == nil {
							return n, true
						}
					}
				}
			}
			return 0, false
		}

		enrichMatch := func(m bson.M) {
			var westId, eastId int64
			if id, ok := getNumericID(m, "west_rikishi_id", "west_id", "westId", "rikishi1_id"); ok {
				westId = id
			}
			if id, ok := getNumericID(m, "east_rikishi_id", "east_id", "eastId", "rikishi2_id"); ok {
				eastId = id
			}
			if wrk, ok := m["west_rikishi"].(bson.M); ok {
				if s, ok := wrk["s3_url"].(string); ok && s != "" {
					m["west_image"] = s
				} else if s, ok := wrk["pfp_url"].(string); ok && s != "" {
					m["west_image"] = s
				} else if s, ok := wrk["image_url"].(string); ok && s != "" {
					m["west_image"] = s
				}
				if rv, ok := wrk["current_rank"]; ok {
					if rv == nil {
						m["west_rank"] = "NA"
					} else {
						m["west_rank"] = rv
					}
				}
				if h, ok := wrk["current_height"]; ok {
					m["west_current_height"] = h
				}
				if wv, ok := wrk["current_weight"]; ok {
					m["west_current_weight"] = wv
				}
				if wins, ok := wrk["wins"]; ok {
					m["west_wins"] = wins
				}
				if losses, ok := wrk["losses"]; ok {
					m["west_losses"] = losses
				}
				if y, ok := wrk["yusho_count"]; ok {
					m["west_yusho"] = y
				}
				if s, ok := wrk["sansho_count"]; ok {
					m["west_sansho"] = s
				}
				if bd, ok := wrk["birthdate"].(string); ok && bd != "" {
					m["west_birthdate"] = bd
				}
				if heya, ok := wrk["heya"].(string); ok && heya != "" {
					m["west_heya"] = heya
				}
			} else {
				coll := a.Mongo.Collection("rikishi_pages")
				if westId != 0 {
					filter := bson.M{"$or": bson.A{bson.M{"id": westId}, bson.M{"id": int32(westId)}}}
					var rdoc bson.M
					if err := coll.FindOne(ctx, filter).Decode(&rdoc); err == nil {
						rk := rdoc["rikishi"]
						if rkMap, ok := rk.(bson.M); ok {
							if s, ok := rkMap["s3_url"].(string); ok && s != "" {
								m["west_image"] = s
							} else if s, ok := rkMap["pfp_url"].(string); ok && s != "" {
								m["west_image"] = s
							} else if s, ok := rkMap["image_url"].(string); ok && s != "" {
								m["west_image"] = s
							}
							if rv, ok := rkMap["current_rank"]; ok {
								if rv == nil {
									m["west_rank"] = "NA"
								} else {
									m["west_rank"] = rv
								}
							}
							if h, ok := rkMap["current_height"]; ok {
								m["west_current_height"] = h
							}
							if w, ok := rkMap["current_weight"]; ok {
								m["west_current_weight"] = w
							}
							if wins, ok := rkMap["wins"]; ok {
								m["west_wins"] = wins
							}
							if losses, ok := rkMap["losses"]; ok {
								m["west_losses"] = losses
							}
							if y, ok := rkMap["yusho_count"]; ok {
								m["west_yusho"] = y
							}
							if s, ok := rkMap["sansho_count"]; ok {
								m["west_sansho"] = s
							}
							if bd, ok := rkMap["birthdate"].(string); ok && bd != "" {
								m["west_birthdate"] = bd
							}
							if heya, ok := rkMap["heya"].(string); ok && heya != "" {
								m["west_heya"] = heya
							}
						}
					}
				}
			}
			if erk, ok := m["east_rikishi"].(bson.M); ok {
				if s, ok := erk["s3_url"].(string); ok && s != "" {
					m["east_image"] = s
				} else if s, ok := erk["pfp_url"].(string); ok && s != "" {
					m["east_image"] = s
				} else if s, ok := erk["image_url"].(string); ok && s != "" {
					m["east_image"] = s
				}
				if rv, ok := erk["current_rank"]; ok {
					if rv == nil {
						m["east_rank"] = "NA"
					} else {
						m["east_rank"] = rv
					}
				}
				if h, ok := erk["current_height"]; ok {
					m["east_current_height"] = h
				}
				if wv, ok := erk["current_weight"]; ok {
					m["east_current_weight"] = wv
				}
				if wins, ok := erk["wins"]; ok {
					m["east_wins"] = wins
				}
				if losses, ok := erk["losses"]; ok {
					m["east_losses"] = losses
				}
				if y, ok := erk["yusho_count"]; ok {
					m["east_yusho"] = y
				}
				if s, ok := erk["sansho_count"]; ok {
					m["east_sansho"] = s
				}
				if bd, ok := erk["birthdate"].(string); ok && bd != "" {
					m["east_birthdate"] = bd
				}
				if heya, ok := erk["heya"].(string); ok && heya != "" {
					m["east_heya"] = heya
				}
			} else {
				coll := a.Mongo.Collection("rikishi_pages")
				if eastId != 0 {
					filter := bson.M{"$or": bson.A{bson.M{"id": eastId}, bson.M{"id": int32(eastId)}}}
					var rdoc bson.M
					if err := coll.FindOne(ctx, filter).Decode(&rdoc); err == nil {
						rk := rdoc["rikishi"]
						if rkMap, ok := rk.(bson.M); ok {
							if s, ok := rkMap["s3_url"].(string); ok && s != "" {
								m["east_image"] = s
							} else if s, ok := rkMap["pfp_url"].(string); ok && s != "" {
								m["east_image"] = s
							} else if s, ok := rkMap["image_url"].(string); ok && s != "" {
								m["east_image"] = s
							}
							if rv, ok := rkMap["current_rank"]; ok {
								if rv == nil {
									m["east_rank"] = "NA"
								} else {
									m["east_rank"] = rv
								}
							}
							if h, ok := rkMap["current_height"]; ok {
								m["east_current_height"] = h
							}
							if w, ok := rkMap["current_weight"]; ok {
								m["east_current_weight"] = w
							}
							if wins, ok := rkMap["wins"]; ok {
								m["east_wins"] = wins
							}
							if losses, ok := rkMap["losses"]; ok {
								m["east_losses"] = losses
							}
							if y, ok := rkMap["yusho_count"]; ok {
								m["east_yusho"] = y
							}
							if s, ok := rkMap["sansho_count"]; ok {
								m["east_sansho"] = s
							}
							if bd, ok := rkMap["birthdate"].(string); ok && bd != "" {
								m["east_birthdate"] = bd
							}
							if heya, ok := rkMap["heya"].(string); ok && heya != "" {
								m["east_heya"] = heya
							}
						}
					}
				}
			}
		}

		if raw, ok := doc["upcoming_matches"].(bson.A); ok && len(raw) > 0 {
			for i := range raw {
				if m, ok := raw[i].(bson.M); ok {
					enrichMatch(m)
					raw[i] = m
				}
			}
			doc["upcoming_matches"] = raw
		}
	}

	func() {
		defer func() { _ = recover() }()
		tryEnrich()
	}()

	c.JSON(http.StatusOK, doc)
}
