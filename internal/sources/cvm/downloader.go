package cvm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL is the public CVM open-data endpoint for FII monthly informes.
const DefaultBaseURL = "https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS"

// DownloaderOptions configures a Downloader. Zero values are fine.
type DownloaderOptions struct {
	BaseURL    string
	HTTPClient *http.Client
	UserAgent  string
}

// Downloader fetches CVM informe ZIPs over HTTPS.
type Downloader struct {
	baseURL   string
	userAgent string
	http      *http.Client
}

// NewDownloader builds a Downloader.
func NewDownloader(opts DownloaderOptions) *Downloader {
	if opts.BaseURL == "" {
		opts.BaseURL = DefaultBaseURL
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 120 * time.Second}
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "reitbrazil-sync/dev"
	}
	return &Downloader{
		baseURL:   strings.TrimRight(opts.BaseURL, "/"),
		userAgent: opts.UserAgent,
		http:      opts.HTTPClient,
	}
}

// FetchYear downloads inf_mensal_fii_YYYY.zip and returns its bytes.
// The ZIP size is bounded (~20MB per year at the time of writing), so
// buffering in memory is acceptable.
func (d *Downloader) FetchYear(ctx context.Context, year int) ([]byte, error) {
	if year < 2016 {
		return nil, errors.New("cvm: FII informes are only available from 2016 onwards")
	}
	url := fmt.Sprintf("%s/inf_mensal_fii_%d.zip", d.baseURL, year)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", d.userAgent)
	req.Header.Set("Accept", "application/zip, application/octet-stream")

	resp, err := d.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("cvm %s: %d %s", url, resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}
