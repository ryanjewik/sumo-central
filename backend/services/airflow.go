package services

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

type AirflowService struct {
	BaseURL string
}

func NewAirflowService(url string) AirflowService {
	return AirflowService{BaseURL: url}
}

func (s AirflowService) Trigger(payload map[string]any) {
	body, _ := json.Marshal(payload)
	// adjust endpoint to your Airflow API
	req, err := http.NewRequest("POST", s.BaseURL+"/api/v1/dags/sumo_dag/dagRuns", bytes.NewReader(body))
	if err != nil {
		log.Println("airflow req err:", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("airflow call err:", err)
		return
	}
	defer resp.Body.Close()

	log.Println("airflow responded with", resp.StatusCode)
}
