package rules

import (
	"testing"
	"time"
)

func TestReliabilitySmoothing(t *testing.T) {
	cases := []struct {
		completed, noShow, late int
		want                    float64
	}{
		{0, 0, 0, 0.9},   // 新机构保守先验
		{10, 0, 0, 0.95}, // 完成历史稀释先验
		{0, 1, 0, 0.75},  // 一次爽约显著拉低
	}
	for _, c := range cases {
		if got := Reliability(c.completed, c.noShow, c.late); got != c.want {
			t.Errorf("Reliability(%d,%d,%d)=%.3f want %.3f",
				c.completed, c.noShow, c.late, got, c.want)
		}
	}
}

func TestEvaluateBlocks(t *testing.T) {
	now := time.Now()
	base := EligibilityInput{
		DistanceKm: 2, RadiusKm: 8, BoxesRequested: 6, BoxesBookedToday: 0,
		DailyCapacity: 20, BatchNeedsCold: true, HasColdVehicle: true,
		MatchingCategories: 2, ReliabilityScore: 0.95, CompletedCount: 5,
		PlannedPickupAt: now.Add(2 * time.Hour), EstTransitMinutes: 30,
		DonationDeadline: now.Add(6 * time.Hour), Now: now,
	}
	if blocked, _ := Evaluate(base); blocked {
		t.Fatal("base case should pass")
	}

	// 超半径拦截
	bad := base
	bad.DistanceKm = 20
	if blocked, traces := Evaluate(bad); !blocked || traces[0].Status != "block" {
		t.Fatal("radius should block")
	}

	// 无冷藏车拦截冷链批次
	bad = base
	bad.HasColdVehicle = false
	if blocked, _ := Evaluate(bad); !blocked {
		t.Fatal("cold vehicle should block")
	}

	// 需求不匹配拦截（防止囤积、无法及时发放）
	bad = base
	bad.MatchingCategories = 0
	if blocked, _ := Evaluate(bad); !blocked {
		t.Fatal("demand match should block")
	}

	// 时效不可达拦截
	bad = base
	bad.PlannedPickupAt = now.Add(5 * time.Hour)
	bad.EstTransitMinutes = 120
	if blocked, _ := Evaluate(bad); !blocked {
		t.Fatal("late arrival beyond donation deadline should block")
	}

	// 可靠度过低拦截
	bad = base
	bad.ReliabilityScore = 0.4
	if blocked, _ := Evaluate(bad); !blocked {
		t.Fatal("low reliability should block")
	}

	// 运力不足拦截
	bad = base
	bad.BoxesBookedToday = 18
	if blocked, _ := Evaluate(bad); !blocked {
		t.Fatal("capacity should block")
	}
}
