package seed

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// Demo 数据：2 家门店、2 家公益机构、各角色测试账号。
// 密码仅用于本地演示，README 中明示。

type userSeed struct {
	username, password, display, role string
	storeCode, charityCode            string
}

// Run 幂等写入演示数据（以 code/username 判重）。
func Run(ctx context.Context, pool *pgxpool.Pool) error {
	stores := []struct {
		code, name, address, phone string
		lat, lng                   float64
	}{
		{"ST001", "焙香坊·中山公园店", "上海市长宁区长宁路 1018 号", "021-62000001", 31.2243, 121.4243},
		{"ST002", "焙香坊·张江店", "上海市浦东新区祖冲之路 1239 号", "021-62000002", 31.2044, 121.5990},
	}
	for _, s := range stores {
		if _, err := pool.Exec(ctx, `
			INSERT INTO stores(code, name, address, lat, lng, contact_phone)
			VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (code) DO NOTHING`,
			s.code, s.name, s.address, s.lat, s.lng, s.phone); err != nil {
			return err
		}
	}
	charities := []struct {
		code, name, address, phone      string
		lat, lng, radius                float64
		dailyCap, coldVehicles, coldCap int
		recipientTypes                  []string
	}{
		{"CH001", "绿洲食物银行", "上海市长宁区凯旋路 613 号", "021-52000001",
			31.2180, 121.4110, 15, 200, 2, 120,
			[]string{"community_kitchen", "shelter", "elderly_care"}},
		{"CH002", "晨光公益服务中心", "上海市长宁区愚园路 1203 号", "021-52000002",
			31.2205, 121.4301, 8, 60, 0, 0,
			[]string{"community_kitchen"}},
	}
	for _, c := range charities {
		if _, err := pool.Exec(ctx, `
			INSERT INTO charities(code, name, address, lat, lng, service_radius_km,
			    daily_capacity_boxes, cold_chain_vehicles, cold_chain_capacity_boxes,
			    recipient_types, contact_phone)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (code) DO NOTHING`,
			c.code, c.name, c.address, c.lat, c.lng, c.radius,
			c.dailyCap, c.coldVehicles, c.coldCap, c.recipientTypes, c.phone); err != nil {
			return err
		}
	}
	users := []userSeed{
		{"store1.manager", "Store@123", "中山公园店店长", "store_manager", "ST001", ""},
		{"store1.staff", "Store@123", "中山公园店店员", "store_staff", "ST001", ""},
		{"store2.manager", "Store@123", "张江店店长", "store_manager", "ST002", ""},
		{"charity1.staff", "Charity@123", "绿洲食物银行调度员", "charity_staff", "", "CH001"},
		{"charity2.staff", "Charity@123", "晨光公益调度员", "charity_staff", "", "CH002"},
		{"regulator", "Regulator@123", "市场监管局检查员", "regulator", "", ""},
		{"admin", "Admin@123", "平台管理员", "admin", "", ""},
	}
	for _, u := range users {
		hash, err := bcrypt.GenerateFromPassword([]byte(u.password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		var storeID, charityID *int64
		if u.storeCode != "" {
			var id int64
			if err := pool.QueryRow(ctx, `SELECT id FROM stores WHERE code=$1`, u.storeCode).Scan(&id); err == nil {
				storeID = &id
			}
		}
		if u.charityCode != "" {
			var id int64
			if err := pool.QueryRow(ctx, `SELECT id FROM charities WHERE code=$1`, u.charityCode).Scan(&id); err == nil {
				charityID = &id
			}
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO users(username, password_hash, display_name, role, store_id, charity_id)
			VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (username) DO NOTHING`,
			u.username, string(hash), u.display, u.role, storeID, charityID); err != nil {
			return err
		}
	}
	log.Printf("[seed] demo data ready: %d stores, %d charities, %d users",
		len(stores), len(charities), len(users))
	return nil
}
