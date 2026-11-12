package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
)

const MaxPayloadBytes = 1 << 20 // 1 MiB

type TenantID string

func NewTenantID(v string) (TenantID, error) {
	v = strings.TrimSpace(v)
	if len(v) < 3 || len(v) > 64 {
		return "", ErrInvalidTenantID
	}
	return TenantID(v), nil
}

type EndpointID string

func NewEndpointID(v string) (EndpointID, error) {
	v = strings.TrimSpace(v)
	if len(v) < 3 || len(v) > 64 {
		return "", ErrInvalidEndpointID
	}
	return EndpointID(v), nil
}

type DeliveryID string

func NewDeliveryID(v string) (DeliveryID, error) {
	v = strings.TrimSpace(v)
	if len(v) < 3 || len(v) > 64 {
		return "", ErrInvalidDeliveryID
	}
	return DeliveryID(v), nil
}

type IdempotencyKey string

func NewIdempotencyKey(v string) (IdempotencyKey, error) {
	v = strings.TrimSpace(v)
	if len(v) < 1 || len(v) > 128 {
		return "", ErrInvalidIdempotencyKey
	}
	return IdempotencyKey(v), nil
}

type SecretKey string

func NewSecretKey(v string) (SecretKey, error) {
	if len(v) < 16 {
		return "", ErrWeakSecretKey
	}
	return SecretKey(v), nil
}

// SignPayload computes an HMAC-SHA256 signature formatted as "sha256=<hex>" over "<unix_timestamp>.<payload>".
func (s SecretKey) SignPayload(timestamp time.Time, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(s))
	prefix := fmt.Sprintf("%d.", timestamp.Unix())
	mac.Write([]byte(prefix))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature performs a constant-time comparison of a candidate signature.
func (s SecretKey) VerifySignature(timestamp time.Time, payload []byte, candidate string) bool {
	expected := s.SignPayload(timestamp, payload)
	return hmac.Equal([]byte(expected), []byte(candidate))
}

func ValidateTargetURL(raw string) (string, error) {
	u, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", ErrInvalidTargetURL
	}
	return u.String(), nil
}

// BackoffPolicy is an immutable value object that computes retry delays.
type BackoffPolicy struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
		Multiplier:      2.0,
	}
}

// NextDelay calculates the retry wait duration for a 1-indexed attempt number.
func (b BackoffPolicy) NextDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return b.InitialInterval
	}
	factor := math.Pow(b.Multiplier, float64(attempt-1))
	delay := time.Duration(float64(b.InitialInterval) * factor)
	if delay > b.MaxInterval || delay <= 0 {
		return b.MaxInterval
	}
	return delay
}
