package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/auth"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	var ae *apperror.Error
	if errors.As(err, &ae) {
		writeJSON(w, ae.HTTPStatus, ae)
		return
	}
	writeJSON(w, 500, map[string]string{"code": "internal", "message": err.Error()})
}

func decode(r *http.Request, v any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil // 允许空请求体
		}
		return apperror.New(400, "bad_json", "请求体不是合法 JSON: "+err.Error())
	}
	return nil
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearer(r)
		if token == "" {
			writeErr(w, apperror.ErrUnauthorized)
			return
		}
		claims, err := auth.Parse(a.cfg.TokenSecret, token)
		if err != nil {
			writeErr(w, apperror.ErrUnauthorized)
			return
		}
		ctx := auth.WithClaims(r.Context(), claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requireRole(roles ...string) func(http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, r := range roles {
		allowed[r] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, ok := auth.FromContext(r.Context())
			if !ok || !allowed[c.Role] {
				writeErr(w, apperror.ErrForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
