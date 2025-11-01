package handlers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (a *App) HandleWebhook(c *gin.Context) {
	// read body raw so we can verify
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	// Sumo API sends header `X-Webhook-Signature` and expects the signature
	// to be HMAC-SHA256(secret, url + body) as shown in the docs.
	incomingSig := c.GetHeader("X-Webhook-Signature")

	// Build the URL string the API uses for signing. Use the request URL (path + query).
	// If Sumo signs the full host+path, you may need to include the scheme+host;
	// using Request.URL.String() matches their docs which demonstrated writing the url then body.
	urlStr := c.Request.URL.String()

	// Try both path-only URL and full URL (scheme://host+path) because some
	// callers sign using the full destination URL while others may use only
	// the path. Accept either form for compatibility.
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	fullURL := scheme + "://" + c.Request.Host + urlStr

	// compute expected signatures for debugging and clearer logs
	expectedPath := computeExpectedSignature(a.Cfg.WebhookSecret, urlStr, body)
	expectedFull := computeExpectedSignature(a.Cfg.WebhookSecret, fullURL, body)
	// also compute expected signature using the registered destination (what we sent when subscribing)
	expectedRegistered := computeExpectedSignature(a.Cfg.WebhookSecret, a.Cfg.WebhookDest, body)
	// some providers sign using host+path (no scheme). compute that too.
	hostPath := c.Request.Host + urlStr
	expectedHostPath := computeExpectedSignature(a.Cfg.WebhookSecret, hostPath, body)
	// and compute using the registered destination host+path without scheme
	regNoScheme := strings.TrimPrefix(strings.TrimPrefix(a.Cfg.WebhookDest, "https://"), "http://")
	expectedRegisteredHostPath := computeExpectedSignature(a.Cfg.WebhookSecret, regNoScheme, body)

	if !(hmac.Equal([]byte(expectedPath), []byte(incomingSig)) || hmac.Equal([]byte(expectedFull), []byte(incomingSig)) || hmac.Equal([]byte(expectedRegistered), []byte(incomingSig)) || hmac.Equal([]byte(expectedHostPath), []byte(incomingSig)) || hmac.Equal([]byte(expectedRegisteredHostPath), []byte(incomingSig))) {
		// Log detailed debug info to help match what the provider sent.
		// We intentionally do NOT log the secret; we only log header values, expected hex, url variants and a short body sample.
		bodySample := string(body)
		if len(bodySample) > 300 {
			bodySample = bodySample[:300] + "..."
		}
		log.Printf("invalid webhook signature: incoming=%s expected_path=%s expected_full=%s expected_registered=%s expected_hostpath=%s expected_registered_hostpath=%s url_path=%s full_url=%s headers=%v body_sample=%s",
			incomingSig, expectedPath, expectedFull, expectedRegistered, expectedHostPath, expectedRegisteredHostPath, urlStr, fullURL, c.Request.Header, bodySample)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid signature"})
		return
	}

	// at this point body looks like:
	// {
	//   "type": "basho.created",
	//   "payload": {...}
	// }

	// respond with 204 No Content to acknowledge receipt per the docs
	c.Status(http.StatusNoContent)

	// background: trigger airflow, store to mongo, whatever
	go func(rawBody []byte, hdr http.Header) {
		// Unmarshal the incoming envelope: { "type": "...", "payload": "<base64>" }
		var env struct {
			Type    string `json:"type"`
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(rawBody, &env); err != nil {
			log.Printf("webhook: failed to unmarshal envelope: %v", err)
			return
		}

		// Decode base64 payload if present
		var decoded interface{}
		if env.Payload != "" {
			b, err := base64.StdEncoding.DecodeString(env.Payload)
			if err != nil {
				log.Printf("webhook: payload base64 decode failed: %v", err)
			} else {
				// try to unmarshal decoded JSON into a generic map
				if err := json.Unmarshal(b, &decoded); err != nil {
					// not JSON? store as raw string
					decoded = string(b)
				}
			}
		}

		// Log the decoded payload for quick inspection
		if decoded != nil {
			if out, err := json.MarshalIndent(decoded, "", "  "); err == nil {
				log.Printf("webhook: decoded payload for type=%s:\n%s", env.Type, string(out))
			} else {
				log.Printf("webhook: decoded payload (raw) for type=%s: %v", env.Type, decoded)
			}
		} else {
			log.Printf("webhook: decoded payload empty for type=%s", env.Type)
		}

		// Build a record similar to the Python helper: timestamp, type, headers, raw envelope, decoded payload
		record := map[string]interface{}{
			"received_at":     time.Now().UTC().Format("20060102T150405Z"),
			"type":            env.Type,
			"headers":         hdr,
			"raw":             json.RawMessage(rawBody),
			"payload_decoded": decoded,
		}

		// Persist record to a system temp directory so we avoid permission problems
		// inside various container runtime setups. Use os.TempDir() (usually /tmp).
		outDir := filepath.Join(os.TempDir(), "sumo_webhooks")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			log.Printf("webhook: failed to create out dir: %v", err)
			return
		}
		fname := fmt.Sprintf("%s_%s.json", time.Now().UTC().Format("20060102T150405"), env.Type)
		outPath := filepath.Join(outDir, fname)
		outB, _ := json.MarshalIndent(record, "", "  ")
		if err := os.WriteFile(outPath, outB, 0o644); err != nil {
			log.Printf("webhook: write file failed: %v", err)
			return
		}
		log.Printf("webhook: saved decoded payload -> %s", outPath)

		// Trigger Airflow DAG run by POSTing to the Airflow REST API.
		go func(envType string, decoded interface{}) {
			// Build the webhook object we want available inside the DAG as dag_run.conf
			webhookObj := map[string]interface{}{
				"type":    envType,
				"payload": decoded,
			}

			bodyObj := map[string]interface{}{"conf": webhookObj}
			b, _ := json.Marshal(bodyObj)

			// Resolve Airflow API URL and credentials (fall back to service name used in docker-compose)
			airflowHost := os.Getenv("AIRFLOW_API_HOST")
			if airflowHost == "" {
				airflowHost = "http://airflow-webserver:8080"
			} else if !strings.HasPrefix(airflowHost, "http://") && !strings.HasPrefix(airflowHost, "https://") {
				// allow users to set host like "airflow-webserver:8080" -- normalize to include scheme
				airflowHost = "http://" + airflowHost
			}
			dagID := os.Getenv("AIRFLOW_DAG_ID")
			if dagID == "" {
				dagID = "sumo_data_pipeline"
			}
			apiURL := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns", strings.TrimRight(airflowHost, "/"), dagID)

			req, err := http.NewRequest("POST", apiURL, bytes.NewReader(b))
			if err != nil {
				log.Printf("webhook: failed to build airflow request: %v", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")

			// Prefer token auth if provided, otherwise fall back to basic auth
			afUser := os.Getenv("AIRFLOW_USERNAME")
			afPass := os.Getenv("AIRFLOW_PASSWORD")
			afToken := os.Getenv("AIRFLOW_API_TOKEN")
			if afToken != "" {
				req.Header.Set("Authorization", "Bearer "+afToken)
			} else if afUser != "" {
				req.SetBasicAuth(afUser, afPass)
			}

			// Masked auth debug: don't print secrets, but show that an auth header is present
			authHdr := req.Header.Get("Authorization")
			masked := "<none>"
			if authHdr != "" {
				if strings.HasPrefix(authHdr, "Basic ") && len(authHdr) > 12 {
					masked = "Basic " + authHdr[6:12] + "..."
				} else if strings.HasPrefix(authHdr, "Bearer ") {
					masked = "Bearer <set>"
				} else {
					masked = authHdr
				}
			}
			log.Printf("webhook: triggering airflow apiURL=%s auth=%s", apiURL, masked)

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("webhook: airflow trigger failed: %v", err)
				return
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			log.Printf("webhook: airflow trigger response: status=%d body=%s", resp.StatusCode, string(respBody))
		}(env.Type, decoded)

		// Optionally: call other services (a.Sumo, push to Kafka, etc). Keep minimal here.
	}(body, c.Request.Header)
}

// computeExpectedSignature returns the expected hex HMAC for diagnostics (url + body)
func computeExpectedSignature(secret string, urlStr string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(urlStr))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
