package api

import (
	"net/http"

	"bakery-donation/internal/auth"

	"github.com/go-chi/chi/v5"
)

func (a *API) inspect(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	res, err := a.svc.Inspect(r.Context(), chi.URLParam(r, "batchID"), c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	status := 200
	if res.Result == "defect" {
		status = 422
	}
	writeJSON(w, status, res)
}

func (a *API) listInspections(w http.ResponseWriter, r *http.Request) {
	rows, err := a.db.Pool.Query(r.Context(), `
		SELECT i.id::text, i.batch_id::text, b.code, i.inspected_at,
		       i.chain_ok, i.five_vouchers_complete, i.ledger_consistent,
		       i.result, i.findings
		FROM inspections i JOIN donation_batches b ON b.id=i.batch_id
		ORDER BY i.inspected_at DESC LIMIT 100`)
	if err != nil {
		writeErr(w, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, bid, code, result string
		var chainOK, fiveOK, ledgerOK bool
		var findings []string
		var at interface{}
		if err := rows.Scan(&id, &bid, &code, &at, &chainOK, &fiveOK, &ledgerOK, &result, &findings); err != nil {
			writeErr(w, err)
			return
		}
		out = append(out, map[string]any{
			"inspection_id": id, "batch_id": bid, "batch_code": code,
			"inspected_at": at, "chain_ok": chainOK,
			"five_vouchers_complete": fiveOK, "ledger_consistent": ledgerOK,
			"result": result, "findings": findings,
		})
	}
	writeJSON(w, 200, map[string]any{"inspections": out})
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	n, err := a.svc.Reconcile(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"rebuilt_batches": n})
}

func (a *API) sweep(w http.ResponseWriter, r *http.Request) {
	res, err := a.svc.Sweep(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, res)
}
