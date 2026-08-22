//! REST 统一错误模型（§31.6 `forgelink.error.v1`）。
//!
//! `request_id` 在请求进入时生成并贯穿日志与错误响应；错误码稳定、
//! 机器可读（`DEVICE_NOT_FOUND` 等），`message` 为人类可读描述。

use axum::Json;
use axum::http::StatusCode;
use serde::Serialize;

/// 错误响应载荷（§31.6）。
///
/// `code` 为稳定、机器可读的错误码文本：默认取 [`ErrorCode`] 的类别码；
/// 控制链路可通过 [`ApiError::control`] 覆写为引擎稳定码（如
/// `VALUE_OUT_OF_RANGE`），客户端据此精确识别失败原因。
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub schema: &'static str,
    pub code: String,
    pub message: String,
    pub request_id: String,
    pub details: serde_json::Value,
}

impl ErrorResponse {
    pub const SCHEMA: &'static str = "forgelink.error.v1";
}

/// 稳定错误码（§31.6 建议状态码映射）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// 400：请求格式错误（非法路径参数等）。
    BadRequest,
    /// 404：设备或资源不存在。
    DeviceNotFound,
    /// 404：资源路径不存在。
    ResourceNotFound,
    /// 405：路径存在但 HTTP 方法不支持。
    MethodNotAllowed,
    /// 409：状态冲突（本分支无写操作，保留供控制阶段使用）。
    StateConflict,
    /// 500：内部错误（快照获取失败等）。
    InternalError,
    /// 503：运行时暂不可用。
    ServiceUnavailable,
    /// 401：未认证（§90.2：缺失/格式非法/未知的 Bearer Token）。fail-closed，
    /// 不区分具体失败原因以避免凭据探测信息泄露。
    Unauthenticated,
    /// 403：已认证但角色不足（§83 授权失败，`INSUFFICIENT_ROLE`）。
    InsufficientRole,
    /// 422：语义校验失败（§84：请求格式正确但违反 Profile/策略约束——
    /// 未知属性、超范围、参数类型不符、前置条件不满足等）。
    ValidationFailed,
}

impl ErrorCode {
    fn code_str(self) -> &'static str {
        match self {
            Self::BadRequest => "BAD_REQUEST",
            Self::DeviceNotFound => "DEVICE_NOT_FOUND",
            Self::ResourceNotFound => "RESOURCE_NOT_FOUND",
            Self::MethodNotAllowed => "METHOD_NOT_ALLOWED",
            Self::StateConflict => "STATE_CONFLICT",
            Self::InternalError => "INTERNAL_ERROR",
            Self::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::InsufficientRole => "INSUFFICIENT_ROLE",
            Self::ValidationFailed => "VALIDATION_FAILED",
        }
    }

    /// HTTP 状态码映射（§31.6；crate 内映射逻辑共用）。
    pub(crate) fn status(self) -> StatusCode {
        match self {
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::DeviceNotFound | Self::ResourceNotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::StateConflict => StatusCode::CONFLICT,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::InsufficientRole => StatusCode::FORBIDDEN,
            Self::ValidationFailed => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }
}

/// API 错误（实现 `IntoResponse`，统一输出 §31.6 载荷）。
#[derive(Debug, Clone)]
pub struct ApiError {
    pub code: ErrorCode,
    /// 错误码文本覆写：控制链路透传引擎稳定码（§80.1 `ControlError.code`，
    /// 如 `VALUE_OUT_OF_RANGE`）；`None` 时使用 `code` 的默认类别码。
    /// 优先用 [`ApiError::control`] 构造而非直接赋值。
    pub code_override: Option<String>,
    pub message: String,
    pub details: serde_json::Value,
}

impl ApiError {
    /// 请求格式错误（400）。
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::BadRequest,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 设备不存在（404）。
    pub fn device_not_found(device_id: &str) -> Self {
        Self {
            code: ErrorCode::DeviceNotFound,
            code_override: None,
            message: format!("设备 {device_id} 不存在"),
            details: serde_json::json!({ "device_id": device_id }),
        }
    }

    /// 资源路径不存在（404）。
    pub fn resource_not_found(device_id: &str, path: &str) -> Self {
        Self {
            code: ErrorCode::ResourceNotFound,
            code_override: None,
            message: format!("设备 {device_id} 的资源 {path} 不存在"),
            details: serde_json::json!({ "device_id": device_id, "path": path }),
        }
    }

    /// 状态冲突（409）。
    pub fn state_conflict(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::StateConflict,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 内部错误（500）。
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::InternalError,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 运行时不可用（503）。
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::ServiceUnavailable,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 未认证（401，§90.2）：缺失/格式非法/未知的 Bearer Token。
    ///
    /// `message` 必须为固定文案——**不得回显 Token 内容**（§90.2 敏感边界：
    /// Token 明文不得经日志或错误信息泄漏）。
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Unauthenticated,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 角色不足（403，§83）：已认证但授权失败。
    pub fn insufficient_role(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::InsufficientRole,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 语义校验失败（422，§84）：请求格式正确但违反 Profile/策略约束。
    pub fn validation_failed(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::ValidationFailed,
            code_override: None,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 控制链路错误：HTTP 状态由 `code` 决定，信封 `code` 文本透传引擎
    /// 稳定错误码（§80.1，如 `DEVICE_NOT_FOUND`/`QUEUE_FULL`），客户端
    /// 据此精确识别失败原因（§31.6：错误码稳定、机器可读）。
    pub fn control(
        code: ErrorCode,
        engine_code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            code_override: Some(engine_code.into()),
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 信封错误码文本：覆写值优先，否则取 [`ErrorCode`] 类别码。
    pub fn code_text(&self) -> &str {
        match &self.code_override {
            Some(text) => text,
            None => self.code.code_str(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code_text(), self.message)
    }
}

impl std::error::Error for ApiError {}

/// 请求进入时生成的标识（贯穿日志与错误响应）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId(pub String);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// 构造 §31.6 错误响应（`request_id` 由调用方注入）。
pub(crate) fn to_response(err: &ApiError, request_id: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        err.code.status(),
        Json(ErrorResponse {
            schema: ErrorResponse::SCHEMA,
            code: err.code_text().to_owned(),
            message: err.message.clone(),
            request_id: request_id.to_owned(),
            details: err.details.clone(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_payload_has_schema_and_request_id() {
        let err = ApiError::device_not_found("vfd-09");
        let (status, Json(body)) = to_response(&err, "req-1");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.schema, "forgelink.error.v1");
        assert_eq!(body.code, "DEVICE_NOT_FOUND");
        assert_eq!(body.request_id, "req-1");
        assert_eq!(body.details["device_id"], "vfd-09");
    }

    #[test]
    fn status_mapping() {
        assert_eq!(
            ApiError::bad_request("x").code.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::device_not_found("x").code.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::resource_not_found("x", "y").code.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::state_conflict("x").code.status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            ApiError::internal("x").code.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ApiError::unavailable("x").code.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // 控制链路新增类别（§31.6/§83/§84/§90.2）。
        assert_eq!(
            ApiError::unauthenticated("x").code.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ApiError::insufficient_role("x").code.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            ApiError::validation_failed("x").code.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[test]
    fn control_error_passes_engine_code_through() {
        // HTTP 状态由类别决定，信封 code 透传引擎稳定码（§80.1）。
        let err = ApiError::control(
            ErrorCode::ValidationFailed,
            "VALUE_OUT_OF_RANGE",
            "属性值超出范围",
        );
        let (status, body) = to_response(&err, "req-1");
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body.code, "VALUE_OUT_OF_RANGE");

        // 未覆写时使用类别默认码。
        let plain = ApiError::unauthenticated("缺少凭据");
        let (_, body) = to_response(&plain, "req-1");
        assert_eq!(body.code, "UNAUTHENTICATED");
    }
}
