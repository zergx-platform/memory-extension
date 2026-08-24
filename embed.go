package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// embedClient talks to the local embedding service (Ollama + BGE-M3) via its
// OpenAI-compatible /api/embed endpoint. 1024-dimensional.
type embedClient struct {
	base string
	hc   *http.Client
}

func newEmbedClient(base string) *embedClient {
	return &embedClient{base: base, hc: &http.Client{Timeout: 60 * time.Second}}
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

// embed returns the embedding vector for a single text.
func (c *embedClient) embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(embedRequest{Model: "bge-m3", Input: []string{text}})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed call: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed HTTP %d", resp.StatusCode)
	}
	var out embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Embeddings) == 0 || len(out.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("empty embedding")
	}
	return out.Embeddings[0], nil
}

// vectorLit renders a float32 slice as a pgvector literal '[1,2,3,...]'.
func vectorLit(v []float32) string {
	buf := bytes.NewBufferString("[")
	for i, f := range v {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, "%g", f)
	}
	buf.WriteByte(']')
	return buf.String()
}
