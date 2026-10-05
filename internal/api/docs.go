package api

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.yaml
var openapiYAML []byte

func openAPISpecHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(openapiYAML)
	}
}

func openAPISpecJSONHandler() http.HandlerFunc {
	// JSON 由 Swagger UI 侧自行解析 YAML；这里提供同样内容供下载/工具探测
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(openapiYAML)
	}
}

const swaggerUIPage = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <title>临期烘焙食品捐赠冷链凭证服务 · OpenAPI</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
  <style>body{margin:0} .topbar{background:#7a4a1f;color:#fff;padding:10px 18px;font:14px/1.5 sans-serif}</style>
</head>
<body>
  <div class="topbar">连锁烘焙门店临期食品捐赠预约冷链凭证服务 — Swagger UI（离线时可直接访问 <a style="color:#ffd9a8" href="/openapi.yaml">/openapi.yaml</a>）</div>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: '/openapi.yaml',
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis]
    });
  </script>
</body>
</html>`

func swaggerUIHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/docs" || r.URL.Path == "/docs/" || r.URL.Path == "/docs/index.html" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(swaggerUIPage))
			return
		}
		http.NotFound(w, r)
	}
}
