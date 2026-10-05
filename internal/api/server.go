package api

import (
	"context"
	"net/http"
	"time"

	"bakery-donation/internal/config"
	"bakery-donation/internal/database"
	"bakery-donation/internal/projector"
	"bakery-donation/internal/service"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
)

type API struct {
	cfg  config.Config
	db   *database.DB
	rdb  *redis.Client
	svc  *service.Service
	proj *projector.Projector
}

func New(cfg config.Config, db *database.DB, rdb *redis.Client,
	svc *service.Service, proj *projector.Projector) *API {
	return &API{cfg: cfg, db: db, rdb: rdb, svc: svc, proj: proj}
}

func (a *API) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.db.Pool.Ping(ctx); err != nil {
			writeJSON(w, 503, map[string]string{"status": "db_unavailable"})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})

	r.Mount("/docs", swaggerUIHandler())
	r.Get("/openapi.yaml", openAPISpecHandler())
	r.Get("/openapi.json", openAPISpecJSONHandler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/auth/login", a.login)

		r.Group(func(r chi.Router) {
			r.Use(a.authenticate)
			r.Get("/me", a.me)

			// 公示与统计（所有登录角色可见）
			r.Get("/ledger", a.listLedger)
			r.Get("/stats", a.stats)
			r.Get("/batches/{batchID}", a.getBatch)
			r.Get("/batches/{batchID}/vouchers", a.getVouchers)
			r.Get("/exceptions", a.listExceptions)

			// 门店
			r.Group(func(r chi.Router) {
				r.Use(requireRole("store_staff", "platform_admin"))
				r.Get("/stores/me/batches", a.listMyStoreBatches)
				r.Post("/batches/report", a.reportBatch)
				r.Post("/batches/{batchID}/decision", a.decideReservation)
				r.Post("/batches/{batchID}/verify-receiver", a.verifyReceiver)
				r.Post("/batches/{batchID}/scan-box", a.scanBox)
				r.Post("/batches/{batchID}/complete-check", a.completeCheck)
				r.Post("/batches/{batchID}/sold-out", a.markSoldOut)
				r.Post("/batches/{batchID}/cancel", a.cancelBatch)
				r.Post("/batches/{batchID}/release", a.releaseBatch)
			})

			// 交接签收：门店与机构双方（路由只注册一次，角色在处理器内区分）
			r.With(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					allow := requireRole("store_staff", "charity_staff", "platform_admin")
					allow(next).ServeHTTP(w, req)
				})
			}).Post("/batches/{batchID}/sign", a.signHandover)

			// 公益机构
			r.Group(func(r chi.Router) {
				r.Use(requireRole("charity_staff", "platform_admin"))
				r.Get("/charities/me/open-batches", a.openBatches)
				r.Post("/reservations", a.reserve)
				r.Post("/reservations/{reservationID}/cancel", a.cancelReservation)
				r.Post("/reservations/{reservationID}/arrive", a.arrivePickup)
				r.Post("/batches/{batchID}/temperatures", a.recordTemp)
				r.Post("/delivery-plans", a.uploadPlan)
				r.Post("/distributions", a.recordDistribution)
				r.Post("/batches/{batchID}/spoiled-feedback", a.spoiled)
			})

			// 监管
			r.Group(func(r chi.Router) {
				r.Use(requireRole("regulator", "platform_admin"))
				r.Post("/batches/{batchID}/inspections", a.inspect)
				r.Get("/inspections", a.listInspections)
			})

			// 平台管理
			r.Group(func(r chi.Router) {
				r.Use(requireRole("platform_admin"))
				r.Post("/admin/reconcile", a.reconcile)
				r.Post("/admin/sweep", a.sweep)
			})
		})
	})

	return r
}
