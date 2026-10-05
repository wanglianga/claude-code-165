package domain

import (
	"fmt"
	"math"
	"time"
)

// HaversineKm 计算两个经纬度之间的球面距离（公里）。
func HaversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKm = 6371.0088
	toRad := func(d float64) float64 { return d * math.Pi / 180 }
	dLat := toRad(lat2 - lat1)
	dLng := toRad(lng2 - lng1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*math.Sin(dLng/2)*math.Sin(dLng/2)
	return earthRadiusKm * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// ReserveCheckInput 汇总预约前全部规则校验所需的输入。
type ReserveCheckInput struct {
	Batch               Batch
	Charity             Charity
	ReservedTodayBoxes  int // 机构当日已生效预约占用箱数
	ColdReservedToday   int // 机构当日已生效冷链预约占用箱数
	RequestBoxes        int
	ColdChainVehicle    bool // 本次预约是否安排冷藏车
	PlannedPickupAt     time.Time
	Now                 time.Time
	LargeReleaseRatio   float64
	LargeReleaseMaxWait time.Duration
}

// CheckReservation 执行预约规则：服务半径、接收能力、冷链能力、时限、
// 以及“门店临时释放库存越多，越要校验机构及时接收能力”的大批量规则。
// 返回 nil 表示通过，否则返回 *AppError（422）。
func CheckReservation(in ReserveCheckInput) *AppError {
	b := in.Batch
	c := in.Charity

	if b.Status != BatchReported {
		return ErrConflict(fmt.Sprintf("批次当前状态为 %s，不可预约", b.Status))
	}
	if !in.Now.Before(b.LatestPickupAt) {
		return ErrUnprocessable("批次已超过最晚取走时间，不可预约")
	}
	if !b.AllergenConfirmed {
		return ErrUnprocessable("批次过敏原信息未确认，存在食品安全风险，暂不可预约")
	}
	if in.RequestBoxes <= 0 || in.RequestBoxes > b.TotalBoxes {
		return ErrUnprocessable(fmt.Sprintf("预约箱数须为 1~%d", b.TotalBoxes))
	}

	dist := HaversineKm(b.Lat, b.Lng, c.Lat, c.Lng)
	if dist > c.ServiceRadiusKm {
		return ErrUnprocessable(fmt.Sprintf("门店距离 %.1fkm 超出机构服务半径 %.1fkm", dist, c.ServiceRadiusKm))
	}
	if in.ReservedTodayBoxes+in.RequestBoxes > c.DailyCapacityBoxes {
		return ErrUnprocessable(fmt.Sprintf("超出机构当日接收能力（%d/%d 箱）",
			in.ReservedTodayBoxes+in.RequestBoxes, c.DailyCapacityBoxes))
	}

	if b.RequiresColdChain {
		if !in.ColdChainVehicle {
			return ErrUnprocessable("该批次需冷链运输，预约必须安排冷藏车")
		}
		if c.ColdChainVehicles <= 0 {
			return ErrUnprocessable("机构无冷藏车辆，无法接收需冷链批次")
		}
		if in.ColdReservedToday+in.RequestBoxes > c.ColdChainCapacityBoxes {
			return ErrUnprocessable(fmt.Sprintf("超出机构当日冷链接收能力（%d/%d 箱）",
				in.ColdReservedToday+in.RequestBoxes, c.ColdChainCapacityBoxes))
		}
	}

	if in.PlannedPickupAt.After(b.LatestPickupAt) {
		return ErrUnprocessable("计划取货时间晚于批次最晚取走时间")
	}
	if in.PlannedPickupAt.Before(in.Now.Add(-5 * time.Minute)) {
		return ErrUnprocessable("计划取货时间早于当前时间")
	}

	// 大批量临时释放：占用机构当日能力比例越高，越要求快速上门，
	// 避免机构“吃不下、发不出”导致临期食品在公益流向中失控。
	largeThreshold := int(math.Ceil(float64(c.DailyCapacityBoxes) * in.LargeReleaseRatio))
	if in.RequestBoxes >= largeThreshold {
		deadline := in.Now.Add(in.LargeReleaseMaxWait)
		if in.PlannedPickupAt.After(deadline) {
			return ErrUnprocessable(fmt.Sprintf(
				"单次预约 %d 箱达到机构日能力 %.0f%%，须在 %s 前取货以保障及时接收与发放",
				in.RequestBoxes, in.LargeReleaseRatio*100, deadline.Format("15:04")))
		}
	}
	return nil
}

// CheckTemp 校验实测温度是否符合批次温控要求；返回是否超标。
func CheckTemp(b *Batch, tempC float64) bool {
	if !b.RequiresColdChain || b.StorageTempMaxC == nil {
		return true
	}
	return tempC <= *b.StorageTempMaxC
}
