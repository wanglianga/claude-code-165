package apperror

import "fmt"

// Error 业务规则错误，携带机器可读 code 与 HTTP 状态。
type Error struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"-"`
	Details    any    `json:"details,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func New(status int, code, msg string) *Error {
	return &Error{HTTPStatus: status, Code: code, Message: msg}
}

func (e *Error) WithDetails(d any) *Error {
	e.Details = d
	return e
}

var (
	ErrUnauthorized = New(401, "unauthorized", "未认证或登录已失效")
	ErrForbidden    = New(403, "forbidden", "无权执行该操作")
	ErrNotFound     = New(404, "not_found", "资源不存在")
	ErrConflict     = New(409, "conflict", "资源状态冲突")
)
