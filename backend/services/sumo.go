package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type SumoService struct {
	baseURL string
	secret  string
	client  *http.Client
}

func NewSumoService(baseURL, secret string) SumoService {
	return SumoService{
		baseURL: baseURL,
		secret:  secret,
		client:  &http.Client{Timeout: 15 * time.Second},
	}
}

// matches the screenshot format
type SubscribeRequest struct {
	Name          string          `json:"name"`
	Destination   string          `json:"destination"`
	Secret        string          `json:"secret"`
	Subscriptions map[string]bool `json:"subscriptions"`
}

func (s SumoService) SubscribeWebhook(ctx context.Context, reqBody SubscribeRequest) error {
	b, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	// helpful debug: log the outgoing subscribe payload
	log.Printf("attempting subscribe payload: %s\n", string(b))

	url := fmt.Sprintf("%s/api/webhook/subscribe", s.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// read response body for better error messages
	respBody, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("subscribe failed: %s - %s", resp.Status, string(respBody))
	}
	// optional: log success response body
	log.Printf("subscribe success: %s", string(respBody))
	return nil
}
