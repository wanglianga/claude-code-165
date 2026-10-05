package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type Claims struct {
	UserID  string `json:"uid"`
	Role    string `json:"role"`
	Store   string `json:"store,omitempty"`
	Charity string `json:"charity,omitempty"`
	Exp     int64  `json:"exp"`
}

func HashPassword(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

func CheckPassword(hash, pw string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func db64(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

func sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Issue 生成 body.sig 形式的不透明签名令牌。
func Issue(secret string, c Claims, ttl time.Duration) string {
	c.Exp = time.Now().Add(ttl).Unix()
	raw, _ := json.Marshal(c)
	body := b64(raw)
	return body + "." + sign(secret, []byte(body))
}

// Parse 校验签名与有效期。
func Parse(secret, token string) (Claims, error) {
	var c Claims
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return c, errors.New("malformed token")
	}
	want := sign(secret, []byte(parts[0]))
	if !hmac.Equal([]byte(want), []byte(parts[1])) {
		return c, errors.New("bad signature")
	}
	raw, err := db64(parts[0])
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, err
	}
	if time.Now().Unix() > c.Exp {
		return c, errors.New("token expired")
	}
	return c, nil
}

type ctxKey int

const claimsKey ctxKey = iota

func WithClaims(ctx context.Context, c Claims) context.Context {
	return context.WithValue(ctx, claimsKey, c)
}

func FromContext(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(claimsKey).(Claims)
	return c, ok
}
