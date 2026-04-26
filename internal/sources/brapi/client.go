package brapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// DefaultBaseURL is the brapi.dev production endpoint.
const DefaultBaseURL = "https://brapi.dev/api"

// DefaultUserAgent identifies the ingestion pipeline in request logs.
var DefaultUserAgent = fmt.Sprintf("reitbrazil-sync/%s (%s)", version(), runtime.Version())

// ClientOptions configures Client. Zero values pick sensible defaults.
type ClientOptions struct {
	BaseURL    string
	Token      string
	RPS        float64       // rate limit; 0 falls back to 3.
	Timeout    time.Duration // per-request; 0 falls back to 30s.
	MaxRetries int           // 4xx/5xx retries; 0 falls back to 3.
	UserAgent  string
	HTTPClient *http.Client
	Logger     *slog.Logger
}

// Client is a minimal brapi.dev HTTP client.
type Client struct {
	baseURL    string
	token      string
	userAgent  string
	maxRetries int
	limiter    *rate.Limiter
	http       *http.Client
	log        *slog.Logger
}

// NewClient builds a Client. Returns an error if Token is empty.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.Token == "" {
		return nil, errors.New("brapi: token is required")
	}
	if opts.BaseURL == "" {
		opts.BaseURL = DefaultBaseURL
	}
	if opts.RPS <= 0 {
		opts.RPS = 3
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 3
	}
	if opts.UserAgent == "" {
		opts.UserAgent = DefaultUserAgent
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: opts.Timeout}
	} else if opts.HTTPClient.Timeout == 0 {
		opts.HTTPClient.Timeout = opts.Timeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	return &Client{
		baseURL:    strings.TrimRight(opts.BaseURL, "/"),
		token:      opts.Token,
		userAgent:  opts.UserAgent,
		maxRetries: opts.MaxRetries,
		limiter:    rate.NewLimiter(rate.Limit(opts.RPS), int(math.Max(1, math.Ceil(opts.RPS)))),
		http:       opts.HTTPClient,
		log:        opts.Logger.With("source", "brapi"),
	}, nil
}

// getJSON fetches a path and decodes the JSON body into out.
// path is joined with baseURL. query is optional (nil for none).
func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u = u + "?" + query.Encode()
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limit wait: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Accept", "application/json")

		start := time.Now()
		resp, err := c.http.Do(req)
		latency := time.Since(start)
		if err != nil {
			lastErr = fmt.Errorf("request: %w", err)
			c.log.WarnContext(ctx, "brapi request failed",
				"url", u, "attempt", attempt, "latency_ms", latency.Milliseconds(), "err", err)
			if attempt == c.maxRetries {
				return lastErr
			}
			sleep(ctx, backoff(attempt))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read body: %w", err)
			if attempt == c.maxRetries {
				return lastErr
			}
			sleep(ctx, backoff(attempt))
			continue
		}
		if closeErr != nil {
			lastErr = fmt.Errorf("close body: %w", closeErr)
			if attempt == c.maxRetries {
				return lastErr
			}
			sleep(ctx, backoff(attempt))
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = &HTTPError{Status: resp.StatusCode, URL: u, Body: truncate(body)}
			c.log.WarnContext(ctx, "brapi retryable status",
				"url", u, "status", resp.StatusCode, "attempt", attempt,
				"latency_ms", latency.Milliseconds())
			if attempt == c.maxRetries {
				return lastErr
			}
			sleep(ctx, backoff(attempt))
			continue
		}

		if resp.StatusCode >= 400 {
			return &HTTPError{Status: resp.StatusCode, URL: u, Body: truncate(body)}
		}

		c.log.DebugContext(ctx, "brapi ok",
			"url", u, "status", resp.StatusCode, "latency_ms", latency.Milliseconds(),
			"bytes", len(body))

		if out == nil {
			return nil
		}
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("decode json: %w (body: %s)", err, truncate(body))
		}
		return nil
	}
	return lastErr
}

// HTTPError captures an HTTP status returned by brapi. Kept small to avoid
// blowing logs on huge 5xx pages.
type HTTPError struct {
	Status int
	URL    string
	Body   string
}

// Error implements error.
func (e *HTTPError) Error() string {
	return fmt.Sprintf("brapi %d %s: %s", e.Status, e.URL, e.Body)
}

func backoff(attempt int) time.Duration {
	base := 500 * time.Millisecond
	return base << attempt
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func truncate(b []byte) string {
	const max = 400
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "...(truncated)"
}

// version is overwritten at build time via -ldflags "-X main.version=...".
func version() string {
	return "dev"
}
