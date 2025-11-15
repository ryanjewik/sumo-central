package handlers

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// StartReconciler starts a background goroutine that periodically persists
// Redis-held per-user predictions into Postgres. Interval is the reconciliation
// period (e.g., 30s). stopCtx cancels the goroutine.
func StartReconciler(a *App, interval time.Duration, stopCtx context.Context) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCtx.Done():
				return
			case <-ticker.C:
				reconcileOnce(a)
			}
		}
	}()
}

// reconcileOnce scans match_users:* keys and upserts per-user predictions
// into the match_predictions Postgres table.
func reconcileOnce(a *App) {
	if a.Redis == nil || a.PG == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	iter := a.Redis.Scan(ctx, 0, "match_users:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// key format: match_users:<match_id>
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		matchIDStr := parts[1]
		matchID, err := strconv.ParseInt(matchIDStr, 10, 64)
		if err != nil {
			continue
		}

		usersMap, err := a.Redis.HGetAll(ctx, key).Result()
		if err != nil && err != redis.Nil {
			// skip this key on error
			continue
		}
		for userID, rikishiStr := range usersMap {
			rikishiID, err := strconv.ParseInt(rikishiStr, 10, 64)
			if err != nil {
				continue
			}
			// upsert prediction
			_, _ = a.PG.Exec(ctx, `
                INSERT INTO match_predictions (user_id, match_id, predicted_winner, prediction_time)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (user_id, match_id) DO UPDATE
                  SET predicted_winner = EXCLUDED.predicted_winner,
                      prediction_time = NOW()
            `, userID, matchID, rikishiID)
		}
	}
}
