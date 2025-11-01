package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
		client:  &http.Client{},
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("subscribe failed: %s", resp.Status)
	}
	return nil
}
