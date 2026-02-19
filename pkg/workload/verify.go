package workload

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func Verify(applierAURL, applierBURL string) error {
	if applierAURL == "" || applierBURL == "" {
		return nil
	}
	hc := &http.Client{Timeout: 2 * time.Second}
	ha, err := fetchHash(hc, strings.TrimSuffix(applierAURL, "/")+"/hash")
	if err != nil {
		return fmt.Errorf("fetch hash A: %w", err)
	}
	hb, err := fetchHash(hc, strings.TrimSuffix(applierBURL, "/")+"/hash")
	if err != nil {
		return fmt.Errorf("fetch hash B: %w", err)
	}
	if ha != hb {
		return fmt.Errorf("stable hash mismatch: A=%s B=%s", ha, hb)
	}
	return nil
}

func fetchHash(client *http.Client, url string) (string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var body struct {
		Hash string `json:"hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.Hash, nil
}
