package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"donation-service/internal/domain"
)

// Login 校验用户名密码，签发访问令牌。
func (s *Service) Login(ctx context.Context, username, password string) (string, *domain.User, time.Time, error) {
	var (
		u    domain.User
		hash string
	)
	err := s.DB.QueryRow(ctx, `
		SELECT id, username, password_hash, display_name, role, store_id, charity_id, created_at
		FROM users WHERE username=$1`, username).Scan(
		&u.ID, &u.Username, &hash, &u.DisplayName, &u.Role, &u.StoreID, &u.CharityID, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, time.Time{}, domain.NewError(401, "BAD_CREDENTIALS", "用户名或密码错误")
	}
	if err != nil {
		return "", nil, time.Time{}, err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return "", nil, time.Time{}, domain.NewError(401, "BAD_CREDENTIALS", "用户名或密码错误")
	}
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, time.Time{}, err
	}
	token := hex.EncodeToString(buf)
	expires := time.Now().Add(s.Cfg.TokenTTL)
	if _, err := s.DB.Exec(ctx,
		`INSERT INTO api_tokens(token, user_id, expires_at) VALUES($1,$2,$3)`,
		token, u.ID, expires); err != nil {
		return "", nil, time.Time{}, err
	}
	return token, &u, expires, nil
}

// Authenticate 按令牌查找用户。
func (s *Service) Authenticate(ctx context.Context, token string) (*domain.User, error) {
	var u domain.User
	err := s.DB.QueryRow(ctx, `
		SELECT u.id, u.username, u.display_name, u.role, u.store_id, u.charity_id, u.created_at
		FROM api_tokens t JOIN users u ON u.id = t.user_id
		WHERE t.token=$1 AND t.expires_at > now()`, token).Scan(
		&u.ID, &u.Username, &u.DisplayName, &u.Role, &u.StoreID, &u.CharityID, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrUnauthorized
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}
