//! REST 统一错误模型（§31.6 `forgelink.error.v1`）。
//!
//! `request_id` 在请求进入时生成并贯穿日志与错误响应；错误码稳定、
//! 机器可读（`DEVICE_NOT_FOUND` 等），`message` 为人类可读描述。

use axum::Json;
use axum::http::StatusCode;
use serde::Serialize;

/// 错误响应载荷（§31.6）。
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub schema: &'static str,
    pub code: &'static str,
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
        }
    }

    fn status(self) -> StatusCode {
        match self {
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::DeviceNotFound | Self::ResourceNotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::StateConflict => StatusCode::CONFLICT,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// API 错误（实现 `IntoResponse`，统一输出 §31.6 载荷）。
#[derive(Debug, Clone)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub details: serde_json::Value,
}

impl ApiError {
    /// 请求格式错误（400）。
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::BadRequest,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 设备不存在（404）。
    pub fn device_not_found(device_id: &str) -> Self {
        Self {
            code: ErrorCode::DeviceNotFound,
            message: format!("设备 {device_id} 不存在"),
            details: serde_json::json!({ "device_id": device_id }),
        }
    }

    /// 资源路径不存在（404）。
    pub fn resource_not_found(device_id: &str, path: &str) -> Self {
        Self {
            code: ErrorCode::ResourceNotFound,
            message: format!("设备 {device_id} 的资源 {path} 不存在"),
            details: serde_json::json!({ "device_id": device_id, "path": path }),
        }
    }

    /// 状态冲突（409；本分支保留，控制阶段使用）。
    pub fn state_conflict(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::StateConflict,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 内部错误（500）。
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::InternalError,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    /// 运行时不可用（503）。
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::ServiceUnavailable,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.code_str(), self.message)
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
            code: err.code.code_str(),
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
    }
}
