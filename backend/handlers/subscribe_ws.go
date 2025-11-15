package handlers

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// allow from any origin; frontend should be secure in production
		return true
	},
}

// MatchUpdatesWS upgrades the connection to a WebSocket and proxies
// messages published to Redis channel `match_updates:<match_id>` to the client.
func (a *App) MatchUpdatesWS(c *gin.Context) {
	matchID := c.Param("id")
	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.Error(err)
		return
	}
	defer conn.Close()

	log.Printf("ws: client connected for match %s", matchID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channel := "match_updates:" + matchID
	pubsub := a.Redis.Subscribe(ctx, channel)
	// ensure subscription is ready
	if _, err := pubsub.Receive(ctx); err != nil {
		// send error and close
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"failed to subscribe"}`))
		_ = pubsub.Close()
		log.Printf("ws: failed to subscribe to %s: %v", channel, err)
		return
	}

	log.Printf("ws: subscribed to redis channel %s for client", channel)

	ch := pubsub.Channel()

	// reader goroutine to detect client close
	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		conn.SetReadLimit(512)
		_ = conn.SetReadDeadline(time.Time{})
		for {
			if _, _, err := conn.NextReader(); err != nil {
				log.Printf("ws: client disconnected for match %s: %v", matchID, err)
				return
			}
		}
	}()

	// forward pubsub messages to websocket
	for {
		select {
		case <-clientClosed:
			_ = pubsub.Close()
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// write payload as-is (should be JSON string from Lua script)
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload)); err != nil {
				log.Printf("ws: write error for match %s: %v", matchID, err)
				_ = pubsub.Close()
				return
			} else {
				log.Printf("ws: forwarded message to client for match %s", matchID)
			}
		case <-ctx.Done():
			_ = pubsub.Close()
			return
		}
	}
}
