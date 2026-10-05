package rules

import (
	"fmt"
	"time"
)

// 资格规则阈值
const (
	ReliabilityHardBlock = 0.50 // 可靠度低于此值直接拒绝
	ReliabilityWarn      = 0.75
)

// EligibilityInput 预约资格评估输入（公益效率 + 食品安全同时纳入）
type EligibilityInput struct {
	DistanceKm         float64
	RadiusKm           float64
	BoxesRequested     int
	BoxesBookedToday   int // 当日已占用运力（requested+confirmed）
	DailyCapacity      int
	BatchNeedsCold     bool
	HasColdVehicle     bool
	MatchingCategories int // 与受助人需求匹配的品类数
	ReliabilityScore   float64
	CompletedCount     int
	NoShowCount        int
	LateCount          int
	PlannedPickupAt    time.Time
	EstTransitMinutes  int
	DonationDeadline   time.Time
	Now                time.Time
}

// Trace 单条规则命中明细，随预约留档供审计
type Trace struct {
	Rule    string `json:"rule"`
	Status  string `json:"status"` // pass / warn / block
	Message string `json:"message"`
}

// Evaluate 同时评估公益接收效率与食安可达性。
// 返回 blocked=true 时禁止预约；trace 全量保留。
func Evaluate(in EligibilityInput) (blocked bool, traces []Trace) {
	add := func(rule, status, msg string) {
		traces = append(traces, Trace{Rule: rule, Status: status, Message: msg})
		if status == "block" {
			blocked = true
		}
	}

	// 1. 服务半径
	if in.DistanceKm <= in.RadiusKm {
		add("service_radius", "pass",
			fmt.Sprintf("门店距离 %.2f km，在服务半径 %.2f km 内", in.DistanceKm, in.RadiusKm))
	} else {
		add("service_radius", "block",
			fmt.Sprintf("门店距离 %.2f km，超出服务半径 %.2f km", in.DistanceKm, in.RadiusKm))
	}

	// 2. 当日接收能力（运力）
	remaining := in.DailyCapacity - in.BoxesBookedToday
	if remaining >= in.BoxesRequested {
		add("daily_capacity", "pass",
			fmt.Sprintf("当日剩余运力 %d 箱，满足本批 %d 箱", remaining, in.BoxesRequested))
	} else {
		add("daily_capacity", "block",
			fmt.Sprintf("当日剩余运力仅 %d 箱，无法接收 %d 箱", remaining, in.BoxesRequested))
	}

	// 3. 冷藏车辆
	if !in.BatchNeedsCold {
		add("cold_vehicle", "pass", "本批次无需冷藏运输")
	} else if in.HasColdVehicle {
		add("cold_vehicle", "pass", "机构具备冷藏车辆，满足冷链要求")
	} else {
		add("cold_vehicle", "block", "批次需冷藏但机构无冷藏车辆，存在温控断链风险")
	}

	// 4. 受助人需求匹配（避免机构盲目囤积、无法及时发放）
	if in.MatchingCategories > 0 {
		add("demand_match", "pass",
			fmt.Sprintf("有 %d 个品类与在档受助人需求匹配", in.MatchingCategories))
	} else {
		add("demand_match", "block", "批次品类与该机构在档受助人需求无匹配，无法保证及时发放")
	}

	// 5. 时效可达：计划取货 + 在途时间必须早于可捐赠时限
	arrival := in.PlannedPickupAt.Add(time.Duration(in.EstTransitMinutes) * time.Minute)
	if !in.PlannedPickupAt.After(in.Now) {
		add("pickup_time", "block", "计划取货时间必须晚于当前时间")
	} else if !arrival.After(in.DonationDeadline) {
		add("timely_delivery", "pass",
			fmt.Sprintf("预计首批送达 %s，早于可捐赠时限 %s",
				arrival.Format(time.RFC3339), in.DonationDeadline.Format(time.RFC3339)))
	} else {
		add("timely_delivery", "block",
			fmt.Sprintf("预计送达 %s 晚于可捐赠时限 %s，食品可能在途超期",
				arrival.Format(time.RFC3339), in.DonationDeadline.Format(time.RFC3339)))
	}

	// 6. 机构履约可靠度（公益效率：门店释放越多越要挑有能力的机构）
	switch {
	case in.ReliabilityScore < ReliabilityHardBlock:
		add("reliability", "block",
			fmt.Sprintf("机构可靠度 %.2f 低于红线 %.2f（完成 %d/爽约 %d/迟到 %d）",
				in.ReliabilityScore, ReliabilityHardBlock,
				in.CompletedCount, in.NoShowCount, in.LateCount))
	case in.ReliabilityScore < ReliabilityWarn:
		add("reliability", "warn",
			fmt.Sprintf("机构可靠度 %.2f 偏低（爽约 %d 次），请门店重点确认",
				in.ReliabilityScore, in.NoShowCount))
	default:
		add("reliability", "pass",
			fmt.Sprintf("机构可靠度 %.2f，历史完成 %d 次", in.ReliabilityScore, in.CompletedCount))
	}

	return blocked, traces
}

// Reliability 计算机构可靠度：完成计正权，爽约权重 2、迟到权重 0.5；
// 以 9 次正向、1 次负向的保守先验平滑：无历史的新机构得 0.900，
// 与 charities.reliability_score 默认值一致。
func Reliability(completed, noShow, late int) float64 {
	const priorGood, priorBad = 9.0, 1.0
	good := priorGood + float64(completed)
	bad := priorBad + float64(noShow)*2 + float64(late)*0.5
	score := good / (good + bad)
	return float64(int(score*1000+0.5)) / 1000
}
