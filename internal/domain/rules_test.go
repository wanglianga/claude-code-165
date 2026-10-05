package domain

import (
	"testing"
	"time"
)

func TestHaversineKm(t *testing.T) {
	// 上海人民广场 -> 徐家汇 约 7km
	d := HaversineKm(31.2304, 121.4737, 31.1885, 121.4365)
	if d < 5 || d > 9 {
		t.Fatalf("unexpected distance: %v", d)
	}
	if HaversineKm(31.0, 121.0, 31.0, 121.0) != 0 {
		t.Fatal("same point should be 0")
	}
}

func baseBatch() Batch {
	now := time.Now()
	return Batch{
		ID: 1, Status: BatchReported, TotalBoxes: 10,
		AllergenConfirmed: true, RequiresColdChain: false,
		Lat: 31.2243, Lng: 121.4243,
		LatestPickupAt: now.Add(4 * time.Hour),
	}
}

func baseCharity() Charity {
	return Charity{
		ID: 1, ServiceRadiusKm: 15, DailyCapacityBoxes: 100,
		ColdChainVehicles: 1, ColdChainCapacityBoxes: 50,
		Lat: 31.2180, Lng: 121.4110,
	}
}

func TestCheckReservationOK(t *testing.T) {
	now := time.Now()
	err := CheckReservation(ReserveCheckInput{
		Batch: baseBatch(), Charity: baseCharity(),
		RequestBoxes: 5, PlannedPickupAt: now.Add(time.Hour), Now: now,
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err != nil {
		t.Fatalf("expect pass, got %v", err)
	}
}

func TestCheckReservationAllergenMissing(t *testing.T) {
	b := baseBatch()
	b.AllergenConfirmed = false
	err := CheckReservation(ReserveCheckInput{
		Batch: b, Charity: baseCharity(), RequestBoxes: 1,
		PlannedPickupAt: time.Now().Add(time.Hour), Now: time.Now(),
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err == nil || err.Status != 422 {
		t.Fatalf("expect 422 allergen, got %v", err)
	}
}

func TestCheckReservationRadius(t *testing.T) {
	c := baseCharity()
	c.Lat, c.Lng = 31.0, 120.0 // 距门店约 40km
	err := CheckReservation(ReserveCheckInput{
		Batch: baseBatch(), Charity: c, RequestBoxes: 1,
		PlannedPickupAt: time.Now().Add(time.Hour), Now: time.Now(),
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err == nil {
		t.Fatal("expect radius rejection")
	}
}

func TestCheckReservationCapacity(t *testing.T) {
	err := CheckReservation(ReserveCheckInput{
		Batch: baseBatch(), Charity: baseCharity(),
		ReservedTodayBoxes: 98, RequestBoxes: 5,
		PlannedPickupAt: time.Now().Add(time.Hour), Now: time.Now(),
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err == nil {
		t.Fatal("expect capacity rejection")
	}
}

func TestCheckReservationColdChain(t *testing.T) {
	b := baseBatch()
	b.RequiresColdChain = true
	c := baseCharity()
	c.ColdChainVehicles = 0
	now := time.Now()
	err := CheckReservation(ReserveCheckInput{
		Batch: b, Charity: c, RequestBoxes: 1, ColdChainVehicle: true,
		PlannedPickupAt: now.Add(time.Hour), Now: now,
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err == nil {
		t.Fatal("expect cold chain vehicle rejection")
	}
	c.ColdChainVehicles = 1
	err = CheckReservation(ReserveCheckInput{
		Batch: b, Charity: c, RequestBoxes: 1, ColdChainVehicle: false,
		PlannedPickupAt: now.Add(time.Hour), Now: now,
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err == nil {
		t.Fatal("expect cold chain required rejection")
	}
}

func TestCheckReservationLargeRelease(t *testing.T) {
	now := time.Now()
	// 单次 60 箱 >= 机构日能力 50%，要求 3h 内取货
	err := CheckReservation(ReserveCheckInput{
		Batch: Batch{ID: 1, Status: BatchReported, TotalBoxes: 60, AllergenConfirmed: true,
			Lat: 31.2243, Lng: 121.4243, LatestPickupAt: now.Add(6 * time.Hour)},
		Charity: baseCharity(), RequestBoxes: 60,
		PlannedPickupAt: now.Add(5 * time.Hour), Now: now,
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err == nil {
		t.Fatal("expect large release time rejection")
	}
	err = CheckReservation(ReserveCheckInput{
		Batch: Batch{ID: 1, Status: BatchReported, TotalBoxes: 60, AllergenConfirmed: true,
			Lat: 31.2243, Lng: 121.4243, LatestPickupAt: now.Add(6 * time.Hour)},
		Charity: baseCharity(), RequestBoxes: 60,
		PlannedPickupAt: now.Add(2 * time.Hour), Now: now,
		LargeReleaseRatio: 0.5, LargeReleaseMaxWait: 3 * time.Hour,
	})
	if err != nil {
		t.Fatalf("expect pass for quick pickup, got %v", err)
	}
}

func TestStateMachine(t *testing.T) {
	if !CanTransition(BatchReported, BatchReserved) {
		t.Fatal("reported->reserved should be legal")
	}
	if CanTransition(BatchDistributed, BatchReported) {
		t.Fatal("distributed->reported should be illegal")
	}
	if CanTransition(BatchCancelled, BatchReserved) {
		t.Fatal("cancelled is terminal")
	}
}

func TestVoucherChain(t *testing.T) {
	now := time.Now()
	payload := map[string]any{"a": 1, "b": "x"}
	p1 := MarshalPayload(payload)
	h1 := ComputeVoucherHash("", VoucherSource, p1, now)
	v1 := Voucher{Type: VoucherSource, Payload: payload, Hash: h1, PrevHash: "", CreatedAt: now}
	p2 := MarshalPayload(map[string]any{"c": true})
	h2 := ComputeVoucherHash(h1, VoucherHandover, p2, now.Add(time.Second))
	v2 := Voucher{Type: VoucherHandover, Payload: map[string]any{"c": true}, Hash: h2, PrevHash: h1, CreatedAt: now.Add(time.Second)}
	if !VerifyChain([]Voucher{v1, v2}) {
		t.Fatal("chain should verify")
	}
	v2.Hash = h1 // 篡改
	if VerifyChain([]Voucher{v1, v2}) {
		t.Fatal("tampered chain must fail")
	}
}

func TestCheckTemp(t *testing.T) {
	max := 8.0
	b := &Batch{RequiresColdChain: true, StorageTempMaxC: &max}
	if CheckTemp(b, 5.0) != true {
		t.Fatal("5C within limit")
	}
	if CheckTemp(b, 9.0) != false {
		t.Fatal("9C exceeds limit")
	}
	nb := &Batch{RequiresColdChain: false}
	if !CheckTemp(nb, 30.0) {
		t.Fatal("non-cold-chain batch has no temp limit")
	}
}
