package workload

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func Verify(applierAURL, applierBURL string) error {
	return VerifyEventually(applierAURL, applierBURL, 2*time.Minute, 500*time.Millisecond)
}

func VerifyEventually(applierAURL, applierBURL string, timeout, interval time.Duration) error {
	if applierAURL == "" || applierBURL == "" {
		return nil
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	hc := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)

	var lastErr error
	for time.Now().Before(deadline) {
		ha, err := fetchHash(hc, strings.TrimSuffix(applierAURL, "/")+"/hash")
		if err != nil {
			lastErr = fmt.Errorf("fetch hash A: %w", err)
			time.Sleep(interval)
			continue
		}
		hb, err := fetchHash(hc, strings.TrimSuffix(applierBURL, "/")+"/hash")
		if err != nil {
			lastErr = fmt.Errorf("fetch hash B: %w", err)
			time.Sleep(interval)
			continue
		}
		if ha == hb {
			return nil
		}
		lastErr = fmt.Errorf("stable hash mismatch: A=%s B=%s", ha, hb)
		time.Sleep(interval)
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("verification timed out")
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
