package nchan

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client publishes into the local Nchan instance. Messages never go directly
// to a browser socket: Nchan fans them out to every subscriber of the channel
// held by this nginx worker process.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		http: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (c *Client) Publish(ctx context.Context, channel string, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err := c.publishOnce(ctx, channel, body); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func (c *Client) publishOnce(ctx context.Context, channel string, body []byte) error {
	url := c.baseURL + "/" + channel
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-Publisher-Token", c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("nchan publish %s: status=%d body=%s", channel, resp.StatusCode, bytes.TrimSpace(msg))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
