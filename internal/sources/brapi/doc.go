// Package brapi is the source adapter for the brapi.dev Pro API.
//
// All HTTP calls are centralised in client.go, which applies a rate
// limiter, bounded retries on 429/5xx, and Bearer-token authentication.
// Endpoint-specific logic lives in funds.go, prices.go, dividends.go,
// and fundamentals.go. DTOs returned by the API are kept internal to
// this package in types.go.
package brapi
