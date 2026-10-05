package httpapi

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"donation-service/internal/domain"
	"donation-service/internal/service"
)

// Handler 聚合全部 HTTP 处理器。
type Handler struct {
	svc *service.Service
}

func NewHandler(svc *service.Service) *Handler { return &Handler{svc: svc} }

func parseID(c *gin.Context, name string) (int64, bool) {
	id, err := strconv.ParseInt(c.Param(name), 10, 64)
	if err != nil || id <= 0 {
		writeError(c, domain.ErrBadRequest("路径参数 "+name+" 非法"))
		return 0, false
	}
	return id, true
}

// ---------- 认证 ----------

type loginReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func (h *Handler) Login(c *gin.Context) {
	var req loginReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("用户名与密码必填"))
		return
	}
	token, u, expires, err := h.svc.Login(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"token": token, "expires_at": expires, "user": u})
}

func (h *Handler) Me(c *gin.Context) {
	c.JSON(200, gin.H{"user": currentUser(c)})
}

// ---------- 批次 ----------

type reportBatchReq struct {
	Category          string    `json:"category" binding:"required"`
	Description       string    `json:"description"`
	TotalBoxes        int       `json:"total_boxes" binding:"required"`
	QuantityDesc      string    `json:"quantity_desc"`
	ProducedAt        time.Time `json:"produced_at" binding:"required"`
	ShelfLifeHours    int       `json:"shelf_life_hours" binding:"required"`
	Allergens         []string  `json:"allergens"`
	AllergenConfirmed bool      `json:"allergen_confirmed"`
	RequiresColdChain bool      `json:"requires_cold_chain"`
	StorageTempMaxC   *float64  `json:"storage_temp_max_c"`
	Packaging         string    `json:"packaging"`
	Address           string    `json:"address"`
	Lat               float64   `json:"lat"`
	Lng               float64   `json:"lng"`
	LatestPickupAt    time.Time `json:"latest_pickup_at" binding:"required"`
	Notes             string    `json:"notes"`
}

func (h *Handler) ReportBatch(c *gin.Context) {
	storeID, ok := parseID(c, "storeId")
	if !ok {
		return
	}
	var req reportBatchReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("请求体非法: "+err.Error()))
		return
	}
	batch, boxes, err := h.svc.ReportBatch(c.Request.Context(), currentUser(c), storeID, service.ReportBatchInput{
		Category: req.Category, Description: req.Description, TotalBoxes: req.TotalBoxes,
		QuantityDesc: req.QuantityDesc, ProducedAt: req.ProducedAt, ShelfLifeHours: req.ShelfLifeHours,
		Allergens: req.Allergens, AllergenConfirmed: req.AllergenConfirmed,
		RequiresColdChain: req.RequiresColdChain, StorageTempMaxC: req.StorageTempMaxC,
		Packaging: req.Packaging, Address: req.Address, Lat: req.Lat, Lng: req.Lng,
		LatestPickupAt: req.LatestPickupAt, Notes: req.Notes,
	})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(201, gin.H{"batch": batch, "boxes": boxes})
}

func (h *Handler) ListBatches(c *gin.Context) {
	var storeID *int64
	if s := c.Query("store_id"); s != "" {
		if id, err := strconv.ParseInt(s, 10, 64); err == nil {
			storeID = &id
		}
	}
	batches, err := h.svc.ListBatches(c.Request.Context(), currentUser(c), c.Query("status"), storeID)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"batches": batches})
}

func (h *Handler) GetBatch(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	b, boxes, resv, err := h.svc.GetBatchDetail(c.Request.Context(), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"batch": b, "boxes": boxes, "reservation": resv})
}

type confirmAllergensReq struct {
	Allergens []string `json:"allergens" binding:"required"`
}

func (h *Handler) ConfirmAllergens(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req confirmAllergensReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("allergens 必填"))
		return
	}
	b, err := h.svc.ConfirmAllergens(c.Request.Context(), currentUser(c), id, req.Allergens)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"batch": b})
}

// ---------- 预约 ----------

type reserveReq struct {
	CharityID       int64     `json:"charity_id" binding:"required"`
	Boxes           int       `json:"boxes" binding:"required"`
	PlannedPickupAt time.Time `json:"planned_pickup_at" binding:"required"`
	VehiclePlate    string    `json:"vehicle_plate"`
	DriverName      string    `json:"driver_name"`
	ColdChain       bool      `json:"cold_chain"`
	ReceiverName    string    `json:"receiver_name"`
	ReceiverPhone   string    `json:"receiver_phone"`
	ReceiverIDNo    string    `json:"receiver_id_no"`
}

func (h *Handler) Reserve(c *gin.Context) {
	batchID, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req reserveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("请求体非法: "+err.Error()))
		return
	}
	resv, idempotent, err := h.svc.Reserve(c.Request.Context(), currentUser(c), batchID, service.ReserveInput{
		CharityID: req.CharityID, Boxes: req.Boxes, PlannedPickupAt: req.PlannedPickupAt,
		VehiclePlate: req.VehiclePlate, DriverName: req.DriverName, ColdChain: req.ColdChain,
		ReceiverName: req.ReceiverName, ReceiverPhone: req.ReceiverPhone,
		ReceiverIDNo: req.ReceiverIDNo, IdempotencyKey: c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		writeError(c, err)
		return
	}
	status := 201
	if idempotent {
		status = 200
	}
	c.JSON(status, gin.H{"reservation": resv, "idempotent_replay": idempotent})
}

func (h *Handler) ConfirmReservation(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	resv, err := h.svc.Confirm(c.Request.Context(), currentUser(c), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"reservation": resv})
}

type cancelReq struct {
	Reason string `json:"reason" binding:"required"`
}

func (h *Handler) CancelReservation(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req cancelReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("取消原因 reason 必填"))
		return
	}
	resv, err := h.svc.CancelReservation(c.Request.Context(), currentUser(c), id, req.Reason)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"reservation": resv})
}

func (h *Handler) GetReservation(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	resv, err := h.svc.GetReservation(c.Request.Context(), currentUser(c), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"reservation": resv})
}

func (h *Handler) HandoverList(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	detail, err := h.svc.HandoverList(c.Request.Context(), currentUser(c), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, detail)
}

// ---------- 交接 ----------

func (h *Handler) StartHandover(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	hv, err := h.svc.StartHandover(c.Request.Context(), currentUser(c), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(201, gin.H{"handover": hv})
}

type scanReq struct {
	BoxNo      string   `json:"box_no" binding:"required"`
	TempC      *float64 `json:"temp_c"`
	SealIntact *bool    `json:"seal_intact"`
}

func (h *Handler) ScanBox(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req scanReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("box_no 必填"))
		return
	}
	scan, created, err := h.svc.ScanBox(c.Request.Context(), currentUser(c), id, req.BoxNo, req.TempC, req.SealIntact)
	if err != nil {
		writeError(c, err)
		return
	}
	status := 200
	if created {
		status = 201
	}
	c.JSON(status, gin.H{"scan": scan})
}

type completeHandoverReq struct {
	ReceiverName string   `json:"receiver_name" binding:"required"`
	ReceiverIDNo string   `json:"receiver_id_no" binding:"required"`
	OverallTempC *float64 `json:"overall_temp_c"`
	SealIntact   *bool    `json:"seal_intact" binding:"required"`
}

func (h *Handler) CompleteHandover(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req completeHandoverReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("请求体非法: "+err.Error()))
		return
	}
	hv, err := h.svc.CompleteHandover(c.Request.Context(), currentUser(c), id,
		req.ReceiverName, req.ReceiverIDNo, req.OverallTempC, req.SealIntact)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"handover": hv})
}

func (h *Handler) ListScans(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	scans, err := h.svc.ListScans(c.Request.Context(), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"scans": scans})
}

// ---------- 配送与发放 ----------

type deliveryStopReq struct {
	Seq           int      `json:"seq" binding:"required"`
	RecipientType string   `json:"recipient_type" binding:"required"`
	SiteName      string   `json:"site_name" binding:"required"`
	Address       string   `json:"address"`
	Lat           *float64 `json:"lat"`
	Lng           *float64 `json:"lng"`
	PlannedBoxes  int      `json:"planned_boxes" binding:"required"`
}

type deliveryPlanReq struct {
	VehiclePlate string            `json:"vehicle_plate" binding:"required"`
	DriverName   string            `json:"driver_name" binding:"required"`
	ColdChain    bool              `json:"cold_chain"`
	Stops        []deliveryStopReq `json:"stops" binding:"required"`
}

func (h *Handler) SubmitDeliveryPlan(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req deliveryPlanReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("请求体非法: "+err.Error()))
		return
	}
	stops := make([]service.DeliveryStopInput, 0, len(req.Stops))
	for _, st := range req.Stops {
		stops = append(stops, service.DeliveryStopInput{
			Seq: st.Seq, RecipientType: st.RecipientType, SiteName: st.SiteName,
			Address: st.Address, Lat: st.Lat, Lng: st.Lng, PlannedBoxes: st.PlannedBoxes,
		})
	}
	plan, out, err := h.svc.SubmitDeliveryPlan(c.Request.Context(), currentUser(c), id,
		req.VehiclePlate, req.DriverName, req.ColdChain, stops)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(201, gin.H{"plan": plan, "stops": out})
}

type completeStopReq struct {
	Boxes        int    `json:"boxes" binding:"required"`
	ReceiverSign string `json:"receiver_sign" binding:"required"`
}

func (h *Handler) CompleteStop(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req completeStopReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("boxes 与 receiver_sign 必填"))
		return
	}
	dist, finished, err := h.svc.CompleteStop(c.Request.Context(), currentUser(c), id, req.Boxes, req.ReceiverSign)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"distribution": dist, "batch_distributed": finished})
}

type feedbackReq struct {
	Status string `json:"status" binding:"required"`
	Note   string `json:"note"`
}

func (h *Handler) SubmitFeedback(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req feedbackReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("status 必填(ok/spoiled)"))
		return
	}
	dist, err := h.svc.SubmitFeedback(c.Request.Context(), currentUser(c), id, req.Status, req.Note)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"distribution": dist})
}

func (h *Handler) GetDelivery(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	detail, err := h.svc.GetDeliveryDetail(c.Request.Context(), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, detail)
}

// ---------- 异常 ----------

type reportExceptionReq struct {
	Type   string         `json:"type" binding:"required"`
	Detail map[string]any `json:"detail"`
}

func (h *Handler) ReportException(c *gin.Context) {
	batchID, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req reportExceptionReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("type 必填"))
		return
	}
	ex, err := h.svc.ReportException(c.Request.Context(), currentUser(c), batchID, req.Type, req.Detail)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(201, gin.H{"exception": ex})
}

type resolveReq struct {
	Resolution string `json:"resolution" binding:"required"`
}

func (h *Handler) ResolveException(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	var req resolveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrBadRequest("resolution 必填"))
		return
	}
	ex, err := h.svc.ResolveException(c.Request.Context(), currentUser(c), id, req.Resolution)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"exception": ex})
}

func (h *Handler) ListExceptions(c *gin.Context) {
	var batchID *int64
	if s := c.Query("batch_id"); s != "" {
		if id, err := strconv.ParseInt(s, 10, 64); err == nil {
			batchID = &id
		}
	}
	list, err := h.svc.ListExceptions(c.Request.Context(), batchID, c.Query("status"))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"exceptions": list})
}

// ---------- 凭证 / 审计 / 公示 ----------

func (h *Handler) ListVouchers(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	vouchers, err := h.svc.ListVouchers(c.Request.Context(), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"vouchers": vouchers, "chain_valid": domain.VerifyChain(vouchers)})
}

func (h *Handler) AuditTrail(c *gin.Context) {
	id, ok := parseID(c, "id")
	if !ok {
		return
	}
	trail, err := h.svc.AuditTrail(c.Request.Context(), id)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, trail)
}

func (h *Handler) ListPublicity(c *gin.Context) {
	list, err := h.svc.ListPublicity(c.Request.Context())
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"donations": list})
}

// ---------- 基础数据 ----------

func (h *Handler) ListStores(c *gin.Context) {
	rows, err := h.svc.DB.Query(c.Request.Context(), `
		SELECT id, code, name, address, lat, lng, contact_phone, created_at FROM stores ORDER BY id`)
	if err != nil {
		writeError(c, err)
		return
	}
	defer rows.Close()
	var out []domain.Store
	for rows.Next() {
		var s domain.Store
		if err := rows.Scan(&s.ID, &s.Code, &s.Name, &s.Address, &s.Lat, &s.Lng, &s.ContactPhone, &s.CreatedAt); err != nil {
			writeError(c, err)
			return
		}
		out = append(out, s)
	}
	c.JSON(200, gin.H{"stores": out})
}

func (h *Handler) ListCharities(c *gin.Context) {
	rows, err := h.svc.DB.Query(c.Request.Context(), `
		SELECT id, code, name, address, lat, lng, service_radius_km, daily_capacity_boxes,
		       cold_chain_vehicles, cold_chain_capacity_boxes, recipient_types, contact_phone, created_at
		FROM charities ORDER BY id`)
	if err != nil {
		writeError(c, err)
		return
	}
	defer rows.Close()
	var out []domain.Charity
	for rows.Next() {
		var ch domain.Charity
		if err := rows.Scan(&ch.ID, &ch.Code, &ch.Name, &ch.Address, &ch.Lat, &ch.Lng,
			&ch.ServiceRadiusKm, &ch.DailyCapacityBoxes, &ch.ColdChainVehicles,
			&ch.ColdChainCapacityBoxes, &ch.RecipientTypes, &ch.ContactPhone, &ch.CreatedAt); err != nil {
			writeError(c, err)
			return
		}
		out = append(out, ch)
	}
	c.JSON(200, gin.H{"charities": out})
}

// ---------- 管理 ----------

func (h *Handler) RunExpiryJob(c *gin.Context) {
	n, err := h.svc.RunExpiryOnce(c.Request.Context())
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(200, gin.H{"processed": n})
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}
