package handlers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (a *App) HandleWebhook(c *gin.Context) {
	// read body raw so we can verify
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	incomingSig := c.GetHeader("X-Sumo-Signature") // guess header name from screenshot, adjust to real one

	if !verifySignature(a.Cfg.WebhookSecret, body, incomingSig) {
		log.Println("invalid webhook signature")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid signature"})
		return
	}

	// at this point body looks like:
	// {
	//   "type": "basho.created",
	//   "payload": {...}
	// }

	// respond fast
	c.JSON(http.StatusOK, gin.H{"status": "ok"})

	// background: trigger airflow, store to mongo, whatever
	go func() {
		// TODO: unmarshal and do something
		// a.Sumo ... or call your airflow service
	}()
}

func verifySignature(secret string, body []byte, incoming string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(incoming))
}
