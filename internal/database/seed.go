package database

import (
	"context"

	"bakery-donation/internal/auth"
)

// SeedUsers 幂等创建演示账号（密码统一 Test@1234）。
func SeedUsers(ctx context.Context, d *DB) error {
	hash, err := auth.HashPassword("Test@1234")
	if err != nil {
		return err
	}
	type acc struct {
		login, name, role, orgCode, orgCol string
	}
	accounts := []acc{
		{"admin", "平台管理员", "platform_admin", "", ""},
		{"regulator", "市场监管检查员", "regulator", "", ""},
		{"store1", "中山公园店店员", "store_staff", "SH001", "store"},
		{"store2", "徐家汇店店员", "store_staff", "SH002", "store"},
		{"charity1", "暖阳中心接收员", "charity_staff", "CH001", "charity"},
		{"charity2", "晚晴互助社接收员", "charity_staff", "CH002", "charity"},
		{"charity3", "远郊接力站接收员", "charity_staff", "CH003", "charity"},
	}
	for _, a := range accounts {
		if a.orgCol == "store" {
			_, err = d.Pool.Exec(ctx, `
				INSERT INTO users(login, password_hash, display_name, role, store_id)
				SELECT $1,$2,$3,$4,id FROM stores WHERE code=$5
				ON CONFLICT (login) DO NOTHING`,
				a.login, hash, a.name, a.role, a.orgCode)
		} else if a.orgCol == "charity" {
			_, err = d.Pool.Exec(ctx, `
				INSERT INTO users(login, password_hash, display_name, role, charity_id)
				SELECT $1,$2,$3,$4,id FROM charities WHERE code=$5
				ON CONFLICT (login) DO NOTHING`,
				a.login, hash, a.name, a.role, a.orgCode)
		} else {
			_, err = d.Pool.Exec(ctx, `
				INSERT INTO users(login, password_hash, display_name, role)
				VALUES ($1,$2,$3,$4)
				ON CONFLICT (login) DO NOTHING`,
				a.login, hash, a.name, a.role)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
