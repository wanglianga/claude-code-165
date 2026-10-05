package api

import (
	"net/http"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/auth"
	"bakery-donation/internal/service"

	"github.com/go-chi/chi/v5"
)

type loginReq struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	var in loginReq
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	var (
		id, hash, name, role string
		storeID, charityID   *string
		status               string
	)
	err := a.db.Pool.QueryRow(r.Context(), `
		SELECT id::text, password_hash, display_name, role, status,
		       store_id::text, charity_id::text
		FROM users WHERE login=$1`, in.Login).
		Scan(&id, &hash, &name, &role, &status, &storeID, &charityID)
	if err != nil {
		writeErr(w, apperror.New(401, "invalid_credentials", "账号或密码错误"))
		return
	}
	if status != "active" || !auth.CheckPassword(hash, in.Password) {
		writeErr(w, apperror.New(401, "invalid_credentials", "账号或密码错误"))
		return
	}
	c := auth.Claims{UserID: id, Role: role}
	if storeID != nil {
		c.Store = *storeID
	}
	if charityID != nil {
		c.Charity = *charityID
	}
	token := auth.Issue(a.cfg.TokenSecret, c, 12*time.Hour)
	writeJSON(w, 200, map[string]any{
		"token": token, "expires_in": 43200,
		"user": map[string]any{
			"id": id, "login": in.Login, "display_name": name, "role": role,
			"store_id": c.Store, "charity_id": c.Charity,
		},
	})
}

func (a *API) me(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	writeJSON(w, 200, map[string]any{
		"user_id": c.UserID, "role": c.Role,
		"store_id": c.Store, "charity_id": c.Charity,
	})
}

// requireBatchStore 校验当前店员归属该批次门店
func (a *API) requireBatchStore(w http.ResponseWriter, r *http.Request, batchID string) bool {
	c, _ := auth.FromContext(r.Context())
	if c.Role == "platform_admin" {
		return true
	}
	var storeID string
	if err := a.db.Pool.QueryRow(r.Context(),
		`SELECT store_id::text FROM donation_batches WHERE id=$1`, batchID).Scan(&storeID); err != nil {
		writeErr(w, apperror.ErrNotFound)
		return false
	}
	if c.Store != storeID {
		writeErr(w, apperror.ErrForbidden)
		return false
	}
	return true
}

// ---------------- 公示 / 查询 ----------------

func (a *API) listLedger(w http.ResponseWriter, r *http.Request) {
	result := r.URL.Query().Get("result")
	rows, err := a.svc.ListLedger(r.Context(), result, 0)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ledger": rows})
}

func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, a.svc.Stats(r.Context()))
}

func (a *API) getBatch(w http.ResponseWriter, r *http.Request) {
	detail, err := a.svc.BatchDetail(r.Context(), chi.URLParam(r, "batchID"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, detail)
}

func (a *API) getVouchers(w http.ResponseWriter, r *http.Request) {
	chain, err := a.svc.GetVoucherChain(r.Context(), chi.URLParam(r, "batchID"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"vouchers": chain})
}

func (a *API) listExceptions(w http.ResponseWriter, r *http.Request) {
	rows, err := a.svc.ListExceptions(r.Context(), r.URL.Query().Get("unresolved") == "true")
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"exceptions": rows})
}

func (a *API) listMyStoreBatches(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	storeID := c.Store
	if storeID == "" {
		writeJSON(w, 200, map[string]any{"batches": []any{}})
		return
	}
	rows, err := a.db.Pool.Query(r.Context(), `
		SELECT id::text, code, title, status, total_boxes, total_quantity,
		       pickup_deadline, donation_deadline, created_at
		FROM donation_batches WHERE store_id=$1
		ORDER BY created_at DESC LIMIT 100`, storeID)
	if err != nil {
		writeErr(w, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, code, title, status string
		var boxes, qty int
		var pickup, deadline, createdAt time.Time
		if err := rows.Scan(&id, &code, &title, &status, &boxes, &qty,
			&pickup, &deadline, &createdAt); err != nil {
			writeErr(w, err)
			return
		}
		out = append(out, map[string]any{
			"batch_id": id, "batch_code": code, "title": title, "status": status,
			"total_boxes": boxes, "total_quantity": qty,
			"pickup_deadline": pickup, "donation_deadline": deadline,
			"created_at": createdAt,
		})
	}
	writeJSON(w, 200, map[string]any{"batches": out})
}

// ---------------- 门店端 ----------------

func (a *API) reportBatch(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	var in service.ReportBatchInput
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if c.Role == "store_staff" {
		in.StoreID = c.Store // 强制使用登录账号所属门店，防越权
	}
	out, err := a.svc.ReportBatch(r.Context(), in, c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, out)
}

func (a *API) decideReservation(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	batchID := chi.URLParam(r, "batchID")
	if !a.requireBatchStore(w, r, batchID) {
		return
	}
	var in struct {
		ReservationID string `json:"reservation_id"`
		Decision      string `json:"decision"`
		Reason        string `json:"reason"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	out, err := a.svc.DecideReservation(r.Context(), in.ReservationID, in.Decision, in.Reason, c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, out)
}

func (a *API) verifyReceiver(w http.ResponseWriter, r *http.Request) {
	if !a.requireBatchStore(w, r, chi.URLParam(r, "batchID")) {
		return
	}
	var in service.ReceiverVerifyInput
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	out, err := a.svc.VerifyReceiver(r.Context(), chi.URLParam(r, "batchID"), in)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, out)
}

func (a *API) scanBox(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	if !a.requireBatchStore(w, r, chi.URLParam(r, "batchID")) {
		return
	}
	var in service.ScanBoxInput
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	out, err := a.svc.ScanBox(r.Context(), chi.URLParam(r, "batchID"), c.UserID, in)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, out)
}

func (a *API) completeCheck(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	if !a.requireBatchStore(w, r, chi.URLParam(r, "batchID")) {
		return
	}
	out, err := a.svc.CompleteHandoverCheck(r.Context(), chi.URLParam(r, "batchID"), c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, out)
}

func (a *API) signHandover(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	batchID := chi.URLParam(r, "batchID")
	if c.Role == "store_staff" && !a.requireBatchStore(w, r, batchID) {
		return
	}
	var in struct {
		Party string `json:"party"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if c.Role == "store_staff" {
		in.Party = "store"
	}
	if c.Role == "charity_staff" {
		in.Party = "charity"
	}
	if err := a.svc.SignHandover(r.Context(), batchID, in.Party, c.UserID); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "signed:" + in.Party})
}

func (a *API) markSoldOut(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	batchID := chi.URLParam(r, "batchID")
	if !a.requireBatchStore(w, r, batchID) {
		return
	}
	var in struct {
		Note string `json:"note"`
	}
	_ = decode(r, &in)
	if err := a.svc.MarkSoldOut(r.Context(), batchID, c.UserID, in.Note); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "sold_out"})
}

func (a *API) cancelBatch(w http.ResponseWriter, r *http.Request) {
	batchID := chi.URLParam(r, "batchID")
	if !a.requireBatchStore(w, r, batchID) {
		return
	}
	var in struct {
		Reason string `json:"reason"`
	}
	_ = decode(r, &in)
	if err := a.svc.CancelBatch(r.Context(), batchID, in.Reason); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "cancelled"})
}

func (a *API) releaseBatch(w http.ResponseWriter, r *http.Request) {
	batchID := chi.URLParam(r, "batchID")
	if !a.requireBatchStore(w, r, batchID) {
		return
	}
	if err := a.svc.ReleaseBatch(r.Context(), batchID); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "open"})
}
