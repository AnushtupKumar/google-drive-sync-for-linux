//! IPC protocol between `syncd` and `syncctl`.
//!
//! ## Framing
//!
//! Every message (both directions) is framed with
//! `tokio_util::codec::LengthDelimitedCodec` using a 4-byte big-endian length
//! prefix followed by a UTF-8 JSON payload.  This gives us:
//!
//! - Zero-copy streaming via `tokio_util` (no busy-loop reads).
//! - Human-debuggable wire format (plain JSON body).
//! - Easy extension: just add a variant to the enums and re-deploy.
//!
//! ## Security
//!
//! The socket is created with mode `0o600` by `syncd`; `syncctl` must run as the
//! same user.  No authentication tokens are used beyond filesystem permissions.
//!
//! ## Streaming responses
//!
//! For `SubscribeTelemetry` the server keeps the connection open and sends
//! `IpcResponse::TelemetryUpdate` frames every 500 ms until the client closes
//! the connection or the daemon shuts down.

use crate::state::TelemetrySnapshot;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use uuid::Uuid;

// ─────────────────────────────────────────────
// Request / Response enums
// ─────────────────────────────────────────────

/// Requests sent from `syncctl` → `syncd`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
pub enum IpcRequest {
    /// Returns the current `TelemetrySnapshot` (single response, then EOF).
    GetStatus,

    /// Opens a persistent subscription; the server streams `TelemetryUpdate`
    /// frames every 500 ms until the client disconnects.
    SubscribeTelemetry,

    /// Pause one (`folder_id = Some(uuid)`) or all (`None`) folders.
    /// `duration_secs = None` → indefinite pause.
    Pause {
        folder_id: Option<Uuid>,
        duration_secs: Option<u64>,
    },

    /// Resume one or all paused folders immediately.
    Resume { folder_id: Option<Uuid> },

    /// Set global upload bandwidth limit.  `bytes_per_sec = 0` removes the cap.
    SetBandwidthLimit { bytes_per_sec: u64 },

    /// Trigger an immediate full reconciliation walk for one or all folders.
    TriggerScan { folder_id: Option<Uuid> },
}

/// Responses sent from `syncd` → `syncctl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum IpcResponse {
    /// Acknowledges a `Pause`, `Resume`, `SetBandwidthLimit`, or `TriggerScan` request.
    Ok { message: String },

    /// Response to `GetStatus`; connection is closed after this.
    StatusSnapshot(TelemetrySnapshot),

    /// Pushed every 500 ms for `SubscribeTelemetry` subscribers.
    TelemetryUpdate(TelemetrySnapshot),

    /// The daemon encountered an error fulfilling the request.
    Error { code: IpcErrorCode, message: String },

    /// Sent as the very last frame when the server is shutting down and will
    /// close the connection.
    ServerShuttingDown,
}

/// Typed error codes for programmatic handling in `syncctl`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IpcErrorCode {
    /// The requested `folder_id` does not exist in the current configuration.
    FolderNotFound,
    /// The request was malformed or contained an invalid argument.
    InvalidRequest,
    /// The daemon is still initialising.
    DaemonNotReady,
    /// An internal daemon error occurred; details in `message`.
    InternalError,
}

// ─────────────────────────────────────────────
// Codec
// ─────────────────────────────────────────────

/// Errors that can arise during codec encode / decode.
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("JSON deserialisation error at offset {offset}: {source}")]
    Json {
        offset: usize,
        source: serde_json::Error,
    },

    #[error("message exceeds maximum frame size ({0} bytes)")]
    FrameTooLarge(usize),
}

/// Maximum accepted frame body size (16 MiB).  Protects against malformed or
/// malicious length prefixes filling daemon RAM.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

// ── IpcRequest codec ──────────────────────────────────────────────────────────

/// A `tokio_util` codec that frames `IpcRequest` values.
///
/// Encoding: 4-byte big-endian length prefix + JSON body.
/// Decoding: reads the length prefix then the JSON body.
pub struct IpcRequestCodec {
    inner: LengthDelimitedCodec,
}

impl IpcRequestCodec {
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_BYTES)
                .length_field_length(4)
                .new_codec(),
        }
    }
}

impl Default for IpcRequestCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for IpcRequestCodec {
    type Item = IpcRequest;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src) {
            Ok(Some(frame)) => {
                serde_json::from_slice(&frame).map(Some).map_err(|e| CodecError::Json {
                    offset: frame.len(),
                    source: e,
                })
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CodecError::Io(e)),
        }
    }
}

impl Encoder<IpcRequest> for IpcRequestCodec {
    type Error = CodecError;

    fn encode(&mut self, item: IpcRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let json = serde_json::to_vec(&item)
            .map_err(|e| CodecError::Json { offset: 0, source: e })?;
        if json.len() > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge(json.len()));
        }
        self.inner
            .encode(Bytes::from(json), dst)
            .map_err(CodecError::Io)
    }
}

// ── IpcResponse codec ─────────────────────────────────────────────────────────

/// A `tokio_util` codec that frames `IpcResponse` values.
pub struct IpcResponseCodec {
    inner: LengthDelimitedCodec,
}

impl IpcResponseCodec {
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_BYTES)
                .length_field_length(4)
                .new_codec(),
        }
    }
}

impl Default for IpcResponseCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for IpcResponseCodec {
    type Item = IpcResponse;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src) {
            Ok(Some(frame)) => {
                serde_json::from_slice(&frame).map(Some).map_err(|e| CodecError::Json {
                    offset: frame.len(),
                    source: e,
                })
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CodecError::Io(e)),
        }
    }
}

impl Encoder<IpcResponse> for IpcResponseCodec {
    type Error = CodecError;

    fn encode(&mut self, item: IpcResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let json = serde_json::to_vec(&item)
            .map_err(|e| CodecError::Json { offset: 0, source: e })?;
        if json.len() > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge(json.len()));
        }
        self.inner
            .encode(Bytes::from(json), dst)
            .map_err(CodecError::Io)
    }
}

// ─────────────────────────────────────────────
// Helper: split a tokio UnixStream into framed halves
// ─────────────────────────────────────────────

use tokio::net::UnixStream;
use tokio_util::codec::{FramedRead, FramedWrite};

/// Splits a `UnixStream` into a framed reader and writer using the IPC codecs.
///
/// The daemon uses this on the *server* side; `syncctl` uses it on the *client* side.
pub fn framed_split(
    stream: UnixStream,
) -> (
    FramedRead<tokio::net::unix::OwnedReadHalf, IpcRequestCodec>,
    FramedWrite<tokio::net::unix::OwnedWriteHalf, IpcResponseCodec>,
) {
    let (read_half, write_half) = stream.into_split();
    (
        FramedRead::new(read_half, IpcRequestCodec::new()),
        FramedWrite::new(write_half, IpcResponseCodec::new()),
    )
}

/// Splits a `UnixStream` into a framed reader and writer for use by `syncctl`.
///
/// The direction is reversed: the client *writes* requests and *reads* responses.
pub fn client_framed_split(
    stream: UnixStream,
) -> (
    FramedRead<tokio::net::unix::OwnedReadHalf, IpcResponseCodec>,
    FramedWrite<tokio::net::unix::OwnedWriteHalf, IpcRequestCodec>,
) {
    let (read_half, write_half) = stream.into_split();
    (
        FramedRead::new(read_half, IpcResponseCodec::new()),
        FramedWrite::new(write_half, IpcRequestCodec::new()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::codec::{Decoder, Encoder};

    fn encode_decode_request(req: IpcRequest) -> IpcRequest {
        let mut codec = IpcRequestCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(req, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    fn encode_decode_response(resp: IpcResponse) -> IpcResponse {
        let mut codec = IpcResponseCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(resp, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn request_roundtrip_get_status() {
        let rt = encode_decode_request(IpcRequest::GetStatus);
        assert!(matches!(rt, IpcRequest::GetStatus));
    }

    #[test]
    fn request_roundtrip_pause() {
        let id = Uuid::new_v4();
        let rt = encode_decode_request(IpcRequest::Pause {
            folder_id: Some(id),
            duration_secs: Some(30),
        });
        assert!(matches!(rt, IpcRequest::Pause { folder_id: Some(_), duration_secs: Some(30) }));
    }

    #[test]
    fn response_roundtrip_ok() {
        let rt = encode_decode_response(IpcResponse::Ok {
            message: "paused".into(),
        });
        assert!(matches!(rt, IpcResponse::Ok { .. }));
    }

    #[test]
    fn response_roundtrip_error() {
        let rt = encode_decode_response(IpcResponse::Error {
            code: IpcErrorCode::FolderNotFound,
            message: "no such folder".into(),
        });
        assert!(matches!(rt, IpcResponse::Error { code: IpcErrorCode::FolderNotFound, .. }));
    }
}
