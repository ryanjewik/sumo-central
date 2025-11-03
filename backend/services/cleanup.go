package services

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartCleanupTicker runs a periodic cleanup job that removes old/expired or revoked refresh tokens.
// interval: how often to run the cleanup (e.g., 24*time.Hour)
// olderThanDays: tokens older than this threshold (applies to revoked tokens and expired tokens trimming)
func StartCleanupTicker(pg *pgxpool.Pool, interval time.Duration, olderThanDays int) {
	go func() {
		// run once immediately
		if err := cleanupOnce(pg, olderThanDays); err != nil {
			log.Println("cleanup error:", err)
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			if err := cleanupOnce(pg, olderThanDays); err != nil {
				log.Println("cleanup error:", err)
			}
		}
	}()
}

func cleanupOnce(pg *pgxpool.Pool, olderThanDays int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// delete revoked tokens older than olderThanDays OR expired tokens older than olderThanDays
	intervalStr := fmt.Sprintf("%d days", olderThanDays)
	query := `DELETE FROM refresh_tokens
WHERE (revoked = true AND created_at < now() - $1::interval)
   OR (expires_at < now() - $1::interval)`

	tag, err := pg.Exec(ctx, query, intervalStr)
	if err != nil {
		return err
	}
	// tag.String() contains status information
	log.Println("refresh token cleanup completed, tag:", tag.String())
	return nil
}
