package httpapi

import (
	"strings"

	"github.com/gin-gonic/gin"

	"donation-service/internal/domain"
	"donation-service/internal/service"
)

const ctxUserKey = "authUser"

// writeError 统一错误响应。
func writeError(c *gin.Context, err error) {
	if appErr, ok := err.(*domain.AppError); ok {
		c.JSON(appErr.Status, gin.H{"error": gin.H{"code": appErr.Code, "message": appErr.Message}})
		return
	}
	c.JSON(500, gin.H{"error": gin.H{"code": "INTERNAL", "message": err.Error()}})
}

// AuthRequired 解析 Bearer 令牌并注入用户。
func AuthRequired(svc *service.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			writeError(c, domain.ErrUnauthorized)
			c.Abort()
			return
		}
		u, err := svc.Authenticate(c.Request.Context(), strings.TrimPrefix(h, "Bearer "))
		if err != nil {
			writeError(c, err)
			c.Abort()
			return
		}
		c.Set(ctxUserKey, u)
		c.Next()
	}
}

// RequireRoles 限定角色。
func RequireRoles(roles ...string) gin.HandlerFunc {
	allowed := map[string]bool{}
	for _, r := range roles {
		allowed[r] = true
	}
	return func(c *gin.Context) {
		u := currentUser(c)
		if u == nil || (!allowed[u.Role] && u.Role != domain.RoleAdmin) {
			writeError(c, domain.ErrForbidden)
			c.Abort()
			return
		}
		c.Next()
	}
}

func currentUser(c *gin.Context) *domain.User {
	v, ok := c.Get(ctxUserKey)
	if !ok {
		return nil
	}
	u, _ := v.(*domain.User)
	return u
}
