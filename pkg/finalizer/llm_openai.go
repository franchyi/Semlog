package finalizer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultOpenAIURL   = "https://api.openai.com/v1/chat/completions"
	defaultOpenAIModel = "gpt-5-mini"
)

type OpenAIClient struct {
	APIURL string
	APIKey string
	Model  string
	Client *http.Client
}

func NewOpenAIClientFromEnv(apiURL, model string, timeout time.Duration) *OpenAIClient {
	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return nil
	}
	if strings.TrimSpace(apiURL) == "" {
		apiURL = defaultOpenAIURL
	}
	if strings.TrimSpace(model) == "" {
		model = defaultOpenAIModel
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &OpenAIClient{
		APIURL: apiURL,
		APIKey: apiKey,
		Model:  model,
		Client: &http.Client{Timeout: timeout},
	}
}

func (c *OpenAIClient) CompleteJSON(ctx context.Context, prompt string, maxTokens int) (string, error) {
	if c == nil {
		return "", fmt.Errorf("nil openai client")
	}
	if c.APIKey == "" {
		return "", fmt.Errorf("missing openai api key")
	}
	if strings.TrimSpace(c.APIURL) == "" {
		c.APIURL = defaultOpenAIURL
	}
	if strings.TrimSpace(c.Model) == "" {
		c.Model = defaultOpenAIModel
	}
	if maxTokens <= 0 {
		maxTokens = 1024
	}

	reqBody := map[string]any{
		"model": c.Model,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a conflict resolution assistant. Output JSON only."},
			{"role": "user", "content": prompt},
		},
		"max_completion_tokens": maxTokens,
	}
	body, _ := json.Marshal(reqBody)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.APIURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := c.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("openai status %d: %s", resp.StatusCode, string(respBody))
	}

	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", err
	}
	if len(out.Choices) == 0 {
		return "", fmt.Errorf("openai response has no choices")
	}
	content := strings.TrimSpace(out.Choices[0].Message.Content)
	if content == "" {
		return "", fmt.Errorf("openai response content is empty")
	}
	return content, nil
}
