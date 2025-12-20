use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::fmt;

/// Main error type for the Colibri rate limiting service
#[derive(Debug)]
pub enum ColibriError {
    /// Configuration or CLI argument errors
    Config(String),

    /// Rate limiting operation errors
    RateLimit(String),

    /// Node networking and communication errors
    Node(String),

    /// API/HTTP related errors
    Api(String),

    /// Gossip protocol errors
    Gossip(GossipError),

    /// System I/O errors
    Io(std::io::Error),

    /// Transport layer errors
    Transport(String),

    /// Serialization/deserialization errors
    Serialization(SerializationError),

    /// Internal lock poisoning or concurrency errors
    Concurrency(String),
}

/// Gossip protocol specific errors
#[derive(Debug)]
pub enum GossipError {
    /// Message parsing or validation errors
    Message(String),

    /// Node ID generation or validation errors
    NodeId(String),

    /// Vector clock synchronization errors
    VectorClock(String),

    /// Scheduler errors
    Scheduler(String),
}

/// Serialization related errors
#[derive(Debug)]
pub enum SerializationError {
    /// JSON serialization/deserialization errors
    Json(serde_json::Error),

    /// Binary serialization/deserialization errors
    BinaryCodec(postcard::Error),
}

impl fmt::Display for ColibriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColibriError::Config(msg) => write!(f, "Configuration error: {}", msg),
            ColibriError::RateLimit(msg) => write!(f, "Rate limiting error: {}", msg),
            ColibriError::Node(msg) => write!(f, "Node error: {}", msg),
            ColibriError::Api(msg) => write!(f, "API error: {}", msg),
            ColibriError::Gossip(err) => write!(f, "Gossip error: {}", err),
            ColibriError::Io(err) => write!(f, "I/O error: {}", err),
            ColibriError::Transport(msg) => write!(f, "Transport error: {}", msg),
            ColibriError::Serialization(err) => write!(f, "Serialization error: {}", err),
            ColibriError::Concurrency(msg) => write!(f, "Concurrency error: {}", msg),
        }
    }
}

impl fmt::Display for GossipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GossipError::Message(msg) => write!(f, "Message: {}", msg),
            GossipError::NodeId(msg) => write!(f, "Node ID: {}", msg),
            GossipError::VectorClock(msg) => write!(f, "Vector clock: {}", msg),
            GossipError::Scheduler(msg) => write!(f, "Scheduler: {}", msg),
        }
    }
}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerializationError::Json(err) => write!(f, "JSON: {}", err),
            SerializationError::BinaryCodec(err) => write!(f, "Binary codec: {}", err),
        }
    }
}

impl std::error::Error for ColibriError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ColibriError::Io(err) => Some(err),
            ColibriError::Serialization(SerializationError::Json(err)) => Some(err),
            ColibriError::Serialization(SerializationError::BinaryCodec(err)) => Some(err),
            _ => None,
        }
    }
}

impl std::error::Error for GossipError {}
impl std::error::Error for SerializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SerializationError::Json(err) => Some(err),
            SerializationError::BinaryCodec(err) => Some(err),
        }
    }
}

// Convenient type alias for Results using our error type
pub type Result<T> = std::result::Result<T, ColibriError>;

// Axum IntoResponse implementation for HTTP error responses
impl IntoResponse for ColibriError {
    fn into_response(self) -> Response {
        let (status_code, error_message) = match &self {
            ColibriError::Config(msg) => (
                StatusCode::BAD_REQUEST,
                format!("Configuration error: {}", msg),
            ),
            ColibriError::RateLimit(msg) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("Rate limit exceeded: {}", msg),
            ),
            ColibriError::Node(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Service temporarily unavailable: {}", msg),
            ),
            ColibriError::Api(msg) => {
                (StatusCode::BAD_REQUEST, format!("Invalid request: {}", msg))
            }
            ColibriError::Gossip(gossip_err) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Cluster communication error: {}", gossip_err),
            ),
            ColibriError::Io(io_err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error: {}", io_err),
            ),
            ColibriError::Transport(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Transport error: {}", msg),
            ),
            ColibriError::Serialization(ser_err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Data processing error: {}", ser_err),
            ),
            ColibriError::Concurrency(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error: {}", msg),
            ),
        };

        // Create a JSON error response
        let error_response = json!({
            "error": {
                "code": status_code.as_u16(),
                "message": error_message,
                "type": match &self {
                    ColibriError::Config(_) => "configuration_error",
                    ColibriError::RateLimit(_) => "rate_limit_exceeded",
                    ColibriError::Node(_) => "node_error",
                    ColibriError::Api(_) => "api_error",
                    ColibriError::Gossip(_) => "gossip_error",
                    ColibriError::Io(_) => "io_error",
                    ColibriError::Transport(_) => "transport_error",
                    ColibriError::Serialization(_) => "serialization_error",
                    ColibriError::Concurrency(_) => "concurrency_error",
                }
            }
        });

        (status_code, Json(error_response)).into_response()
    }
}

impl ColibriError {
    /// Get the appropriate HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self {
            ColibriError::Config(_) => StatusCode::BAD_REQUEST,
            ColibriError::RateLimit(_) => StatusCode::TOO_MANY_REQUESTS,
            ColibriError::Node(_) => StatusCode::SERVICE_UNAVAILABLE,
            ColibriError::Api(_) => StatusCode::BAD_REQUEST,
            ColibriError::Gossip(_) => StatusCode::SERVICE_UNAVAILABLE,
            ColibriError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ColibriError::Transport(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ColibriError::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ColibriError::Concurrency(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Get a user-friendly error message
    pub fn user_message(&self) -> String {
        match self {
            ColibriError::Config(msg) => format!("Configuration error: {}", msg),
            ColibriError::RateLimit(_) => {
                "Rate limit exceeded. Please try again later.".to_string()
            }
            ColibriError::Node(_) => {
                "Service temporarily unavailable. Please try again later.".to_string()
            }
            ColibriError::Api(msg) => format!("Invalid request: {}", msg),
            ColibriError::Gossip(_) => {
                "Service temporarily unavailable due to cluster issues.".to_string()
            }
            ColibriError::Io(_) => "Internal server error. Please try again later.".to_string(),
            ColibriError::Transport(_) => "Transport error. Please try again later.".to_string(),
            ColibriError::Serialization(_) => {
                "Data processing error. Please check your request format.".to_string()
            }
            ColibriError::Concurrency(_) => {
                "Internal server error. Please try again later.".to_string()
            }
        }
    }

    /// Get the error type identifier
    pub fn error_type(&self) -> &'static str {
        match self {
            ColibriError::Config(_) => "configuration_error",
            ColibriError::RateLimit(_) => "rate_limit_exceeded",
            ColibriError::Node(_) => "node_error",
            ColibriError::Api(_) => "api_error",
            ColibriError::Gossip(_) => "gossip_error",
            ColibriError::Io(_) => "io_error",
            ColibriError::Transport(_) => "transport_error",
            ColibriError::Serialization(_) => "serialization_error",
            ColibriError::Concurrency(_) => "concurrency_error",
        }
    }
}

// Conversions from common error types
impl From<std::io::Error> for ColibriError {
    fn from(err: std::io::Error) -> Self {
        ColibriError::Io(err)
    }
}

impl From<serde_json::Error> for ColibriError {
    fn from(err: serde_json::Error) -> Self {
        ColibriError::Serialization(SerializationError::Json(err))
    }
}

impl From<postcard::Error> for ColibriError {
    fn from(err: postcard::Error) -> Self {
        ColibriError::Serialization(SerializationError::BinaryCodec(err))
    }
}
impl From<GossipError> for ColibriError {
    fn from(err: GossipError) -> Self {
        ColibriError::Gossip(err)
    }
}

impl From<SerializationError> for ColibriError {
    fn from(err: SerializationError) -> Self {
        ColibriError::Serialization(err)
    }
}

// Convert from anyhow::Error for gradual migration
impl From<anyhow::Error> for ColibriError {
    fn from(err: anyhow::Error) -> Self {
        // For the gradual migration from anyhow, we'll just categorize based on the string
        let err_str = err.to_string();

        if err_str.contains("I/O") || err_str.contains("io") {
            ColibriError::Io(std::io::Error::other(err_str))
        } else if err_str.contains("JSON") || err_str.contains("json") {
            ColibriError::Serialization(SerializationError::Json(serde_json::Error::io(
                std::io::Error::new(std::io::ErrorKind::InvalidData, err_str),
            )))
        } else if err_str.contains("deserialize") || err_str.contains("serialize") {
            ColibriError::Serialization(SerializationError::BinaryCodec(
                postcard::Error::DeserializeUnexpectedEnd,
            ))
        } else {
            // Fall back to a generic node error
            ColibriError::Node(err_str)
        }
    }
}
impl From<std::num::TryFromIntError> for ColibriError {
    fn from(err: std::num::TryFromIntError) -> Self {
        ColibriError::Node(format!("Integer conversion error: {}", err))
    }
}

impl From<reqwest::Error> for ColibriError {
    fn from(err: reqwest::Error) -> Self {
        ColibriError::Api(err.to_string())
    }
}

impl From<String> for ColibriError {
    fn from(err: String) -> Self {
        ColibriError::Node(err)
    }
}

impl From<StatusCode> for ColibriError {
    fn from(status: StatusCode) -> Self {
        match status {
            StatusCode::TOO_MANY_REQUESTS => {
                ColibriError::RateLimit("Rate limit exceeded".to_string())
            }
            StatusCode::BAD_REQUEST => ColibriError::Api("Bad request".to_string()),
            StatusCode::SERVICE_UNAVAILABLE => {
                ColibriError::Node("Service unavailable".to_string())
            }
            _ => ColibriError::Api(format!("HTTP error: {}", status.as_u16())),
        }
    }
} // Helper macros for common error construction patterns
#[macro_export]
macro_rules! config_error {
    ($msg:expr) => {
        $crate::error::ColibriError::Config($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::ColibriError::Config(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! rate_limit_error {
    ($msg:expr) => {
        $crate::error::ColibriError::RateLimit($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::ColibriError::RateLimit(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! node_error {
    ($msg:expr) => {
        $crate::error::ColibriError::Node($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::ColibriError::Node(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! api_error {
    ($msg:expr) => {
        $crate::error::ColibriError::Api($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::ColibriError::Api(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! concurrency_error {
    ($msg:expr) => {
        $crate::error::ColibriError::Concurrency($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::ColibriError::Concurrency(format!($fmt, $($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let config_err = ColibriError::Config("Invalid port".to_string());
        assert_eq!(config_err.to_string(), "Configuration error: Invalid port");

        let io_err = ColibriError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        ));
        assert!(io_err.to_string().contains("I/O error"));
    }

    #[test]
    fn test_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
        let colibri_err: ColibriError = io_err.into();

        matches!(colibri_err, ColibriError::Io(_));
    }

    #[test]
    fn test_macros() {
        let err = config_error!("Port {} is invalid", 65536);
        assert_eq!(
            err.to_string(),
            "Configuration error: Port 65536 is invalid"
        );

        let err = rate_limit_error!("Client exceeded limit");
        assert_eq!(
            err.to_string(),
            "Rate limiting error: Client exceeded limit"
        );
    }
}
