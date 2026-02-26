// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package rpcclient

import (
	"net/http"
	"strings"
	"sync"
)

// CapturedHeaders stores headers from the last HTTP response.
// Thread-safe for concurrent access.
type CapturedHeaders struct {
	mu      sync.RWMutex
	headers map[string]string
}

// NewCapturedHeaders creates a new CapturedHeaders instance.
func NewCapturedHeaders() *CapturedHeaders {
	return &CapturedHeaders{headers: make(map[string]string)}
}

// Store captures headers from an HTTP response.
// Only headers with "X-Arb-" prefix are stored.
func (c *CapturedHeaders) Store(headers http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = make(map[string]string)
	for key, values := range headers {
		if strings.HasPrefix(key, "X-Arb-") && len(values) > 0 {
			c.headers[key] = values[0]
		}
	}
}

// Get retrieves a specific header value.
func (c *CapturedHeaders) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.headers[key]
	return v, ok
}

// All returns a copy of all captured headers.
func (c *CapturedHeaders) All() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]string, len(c.headers))
	for k, v := range c.headers {
		result[k] = v
	}
	return result
}

// Clear removes all captured headers.
func (c *CapturedHeaders) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = make(map[string]string)
}

// HeaderCaptureTransport wraps an http.RoundTripper to capture response headers.
type HeaderCaptureTransport struct {
	Base     http.RoundTripper
	Captured *CapturedHeaders
}

// NewHeaderCaptureTransport creates a transport that captures X-Arb-* headers.
func NewHeaderCaptureTransport(base http.RoundTripper) *HeaderCaptureTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &HeaderCaptureTransport{
		Base:     base,
		Captured: NewCapturedHeaders(),
	}
}

// RoundTrip executes the HTTP request and captures response headers.
func (t *HeaderCaptureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.Base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	t.Captured.Store(resp.Header)
	return resp, nil
}
