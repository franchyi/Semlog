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
	var lastAHash string
	var lastBHash string
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
		lastAHash = ha
		lastBHash = hb
		if ha == hb {
			return nil
		}
		lastErr = fmt.Errorf("stable hash mismatch: A=%s B=%s", ha, hb)
		time.Sleep(interval)
	}

	if lastAHash != "" && lastBHash != "" && lastAHash != lastBHash {
		aDebug, _ := fetchVerifyDebug(hc, applierAURL)
		bDebug, _ := fetchVerifyDebug(hc, applierBURL)
		lastErr = fmt.Errorf(
			"stable hash mismatch: A=%s B=%s (A.final_records=%d B.final_records=%d A.apply_errors=%d B.apply_errors=%d)",
			lastAHash,
			lastBHash,
			aDebug.FinalRecordsConsumed,
			bDebug.FinalRecordsConsumed,
			aDebug.FinalApplyErrors,
			bDebug.FinalApplyErrors,
		)
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

type verifyDebug struct {
	FinalRecordsConsumed int64 `json:"final_records_consumed"`
	FinalApplyErrors     int64 `json:"final_apply_errors"`
}

func fetchVerifyDebug(client *http.Client, applierURL string) (verifyDebug, error) {
	url := strings.TrimSuffix(applierURL, "/") + "/metrics"
	resp, err := client.Get(url)
	if err != nil {
		return verifyDebug{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return verifyDebug{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body verifyDebug
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return verifyDebug{}, err
	}
	return body, nil
}
