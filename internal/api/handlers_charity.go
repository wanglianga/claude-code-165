package api

import (
	"net/http"

	"bakery-donation/internal/auth"
	"bakery-donation/internal/service"

	"github.com/go-chi/chi/v5"
)

func (a *API) openBatches(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	rows, err := a.svc.ListOpenBatches(r.Context(), c.Charity, r.URL.Query().Get("radius_only") == "true")
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"open_batches": rows})
}

func (a *API) reserve(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	var in service.ReserveInput
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if c.Role == "charity_staff" {
		in.CharityID = c.Charity // 强制归属，防越权
	}
	out, err := a.svc.Reserve(r.Context(), in, c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, out)
}

func (a *API) cancelReservation(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Reason string `json:"reason"`
	}
	_ = decode(r, &in)
	if err := a.svc.CancelReservation(r.Context(),
		chi.URLParam(r, "reservationID"), in.Reason); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "cancelled"})
}

func (a *API) arrivePickup(w http.ResponseWriter, r *http.Request) {
	out, err := a.svc.ArrivePickup(r.Context(),
		chi.URLParam(r, "reservationID"), nil)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, out)
}

func (a *API) recordTemp(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	var in struct {
		Phase   string  `json:"phase"`
		TempC   float64 `json:"temp_c"`
		BoxCode string  `json:"box_code"`
		Note    string  `json:"note"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if err := a.svc.RecordTemp(r.Context(), chi.URLParam(r, "batchID"),
		in.Phase, in.TempC, in.BoxCode, in.Note, c.UserID); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]string{"status": "recorded"})
}

func (a *API) uploadPlan(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	var in service.DeliveryPlanInput
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if c.Role == "charity_staff" {
		in.CharityID = c.Charity
	}
	out, err := a.svc.UploadDeliveryPlan(r.Context(), in, c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, out)
}

func (a *API) recordDistribution(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	var in service.DistributionInput
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if c.Role == "charity_staff" {
		in.CharityID = c.Charity
	}
	out, err := a.svc.RecordDistribution(r.Context(), in, c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, out)
}

func (a *API) spoiled(w http.ResponseWriter, r *http.Request) {
	c, _ := auth.FromContext(r.Context())
	var in struct {
		BoxCode   string `json:"box_code"`
		PointName string `json:"point_name"`
		Note      string `json:"note"`
	}
	if err := decode(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	out, err := a.svc.SpoiledFeedback(r.Context(),
		chi.URLParam(r, "batchID"), in.BoxCode, in.PointName, in.Note, c.UserID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, out)
}
