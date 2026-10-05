package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"donation-service/internal/domain"
	"donation-service/internal/service"
)

// NewRouter 注册全部路由。
func NewRouter(svc *service.Service, openapiYAML []byte) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	// CORS：允许 Swagger UI(8081) 跨域调用 API
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PATCH,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization,Content-Type,Idempotency-Key")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	h := NewHandler(svc)

	r.GET("/healthz", h.Health)
	// OpenAPI 文档（Swagger UI 容器指向该地址）
	r.GET("/openapi.yaml", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/yaml; charset=utf-8", openapiYAML)
	})

	v1 := r.Group("/api/v1")
	v1.POST("/auth/login", h.Login)
	v1.GET("/publicity/donations", h.ListPublicity) // 捐赠公示：公开

	authed := v1.Group("")
	authed.Use(AuthRequired(svc))
	{
		authed.GET("/me", h.Me)
		authed.GET("/stores", h.ListStores)
		authed.GET("/charities", h.ListCharities)

		// 批次
		authed.POST("/stores/:storeId/batches",
			RequireRoles(domain.RoleStoreStaff, domain.RoleStoreManager), h.ReportBatch)
		authed.GET("/batches", h.ListBatches)
		authed.GET("/batches/:id", h.GetBatch)
		authed.PATCH("/batches/:id/allergens",
			RequireRoles(domain.RoleStoreStaff, domain.RoleStoreManager), h.ConfirmAllergens)
		authed.POST("/batches/:id/reserve",
			RequireRoles(domain.RoleCharityStaff), h.Reserve)
		authed.GET("/batches/:id/vouchers", h.ListVouchers)
		authed.POST("/batches/:id/exceptions", h.ReportException)
		authed.GET("/batches/:id/audit",
			RequireRoles(domain.RoleRegulator), h.AuditTrail)

		// 预约
		authed.GET("/reservations/:id", h.GetReservation)
		authed.POST("/reservations/:id/confirm",
			RequireRoles(domain.RoleStoreManager), h.ConfirmReservation)
		authed.POST("/reservations/:id/cancel", h.CancelReservation)
		authed.GET("/reservations/:id/handover-list", h.HandoverList)

		// 交接
		authed.POST("/reservations/:id/handover/start",
			RequireRoles(domain.RoleStoreStaff, domain.RoleStoreManager), h.StartHandover)
		authed.POST("/reservations/:id/handover/scan",
			RequireRoles(domain.RoleStoreStaff, domain.RoleStoreManager), h.ScanBox)
		authed.POST("/reservations/:id/handover/complete",
			RequireRoles(domain.RoleStoreStaff, domain.RoleStoreManager), h.CompleteHandover)
		authed.GET("/reservations/:id/handover/scans", h.ListScans)

		// 配送与发放
		authed.POST("/reservations/:id/delivery-plan",
			RequireRoles(domain.RoleCharityStaff), h.SubmitDeliveryPlan)
		authed.GET("/reservations/:id/delivery", h.GetDelivery)
		authed.POST("/delivery-stops/:id/complete",
			RequireRoles(domain.RoleCharityStaff), h.CompleteStop)
		authed.POST("/distributions/:id/feedback",
			RequireRoles(domain.RoleCharityStaff, domain.RoleRegulator), h.SubmitFeedback)

		// 异常
		authed.GET("/exceptions", h.ListExceptions)
		authed.POST("/exceptions/:id/resolve", h.ResolveException)

		// 管理
		authed.POST("/admin/jobs/expire", RequireRoles(domain.RoleAdmin), h.RunExpiryJob)
	}
	return r
}
