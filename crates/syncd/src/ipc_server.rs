//! Unix Domain Socket IPC server for `syncd`.
//!
//! Serves requests from `syncctl` using length-delimited JSON framing over a UDS
//! located at `$XDG_RUNTIME_DIR/syncd.sock` with POSIX permissions `0600`.

use crate::supervisor::{Supervisor, SupervisorError};
use futures::{SinkExt, StreamExt};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
};
use sync_common::ipc::{
    framed_split, IpcErrorCode, IpcRequest, IpcResponse,
};
use thiserror::Error;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

// ─────────────────────────────────────────────
// Error types
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum IpcServerError {
    #[error("I/O error binding Unix socket at '{path}': {source}")]
    Bind {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed setting socket permissions at '{path}': {source}")]
    Permissions {
        path: PathBuf,
        source: std::io::Error,
    },
}

// ─────────────────────────────────────────────
// Server Runner
// ─────────────────────────────────────────────

/// Runs the IPC listener until `cancel` is fired.
pub async fn run_ipc_server(
    socket_path: PathBuf,
    supervisor: Arc<Supervisor>,
    cancel: CancellationToken,
) -> Result<(), IpcServerError> {
    // Clean up any stale socket from previous crashes
    if socket_path.exists() {
        let _ = fs::remove_file(&socket_path);
    }

    if let Some(parent) = socket_path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let listener = UnixListener::bind(&socket_path).map_err(|e| IpcServerError::Bind {
        path: socket_path.clone(),
        source: e,
    })?;

    // Secure the socket to mode 0600 (owner read/write only)
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).map_err(|e| {
        IpcServerError::Permissions {
            path: socket_path.clone(),
            source: e,
        }
    })?;

    info!(path = ?socket_path, "IPC UnixListener active (mode 0600)");

    let cleanup_path = socket_path.clone();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("IPC server received cancellation signal, stopping listener");
                break;
            }

            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, _peer_addr)) => {
                        let sup = supervisor.clone();
                        let conn_cancel = cancel.child_token();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, sup, conn_cancel).await {
                                debug!("IPC client disconnected or errored: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("IPC accept error: {}", e);
                    }
                }
            }
        }
    }

    // Clean up socket file on shutdown
    if cleanup_path.exists() {
        let _ = fs::remove_file(&cleanup_path);
        debug!(path = ?cleanup_path, "Cleaned up IPC socket file");
    }

    Ok(())
}

// ─────────────────────────────────────────────
// Client Connection Handler
// ─────────────────────────────────────────────

async fn execute_request(
    supervisor: &Supervisor,
    writer: &mut tokio_util::codec::FramedWrite<tokio::net::unix::OwnedWriteHalf, sync_common::ipc::IpcResponseCodec>,
    req: IpcRequest,
) -> anyhow::Result<()> {
    match req {
        IpcRequest::GetStatus => {
            let snapshot = supervisor.get_status().await;
            writer.send(IpcResponse::StatusSnapshot(snapshot)).await?;
        }

        IpcRequest::SubscribeTelemetry => {
            let snapshot = supervisor.get_status().await;
            writer.send(IpcResponse::TelemetryUpdate(snapshot)).await?;
        }

        IpcRequest::Pause { folder_id, duration_secs } => {
            match supervisor.pause(folder_id, duration_secs).await {
                Ok(msg) => {
                    writer.send(IpcResponse::Ok { message: msg }).await?;
                }
                Err(e) => {
                    writer.send(IpcResponse::Error {
                        code: IpcErrorCode::InternalError,
                        message: e.to_string(),
                    }).await?;
                }
            }
        }

        IpcRequest::Resume { folder_id } => {
            match supervisor.resume(folder_id).await {
                Ok(msg) => {
                    writer.send(IpcResponse::Ok { message: msg }).await?;
                }
                Err(e) => {
                    writer.send(IpcResponse::Error {
                        code: IpcErrorCode::InternalError,
                        message: e.to_string(),
                    }).await?;
                }
            }
        }

        IpcRequest::SetBandwidthLimit { bytes_per_sec } => {
            match supervisor.set_bandwidth_limit(bytes_per_sec).await {
                Ok(msg) => {
                    writer.send(IpcResponse::Ok { message: msg }).await?;
                }
                Err(e) => {
                    writer.send(IpcResponse::Error {
                        code: IpcErrorCode::InvalidRequest,
                        message: e.to_string(),
                    }).await?;
                }
            }
        }

        IpcRequest::TriggerScan { folder_id } => {
            match supervisor.trigger_scan(folder_id).await {
                Ok(msg) => {
                    writer.send(IpcResponse::Ok { message: msg }).await?;
                }
                Err(SupervisorError::FolderNotFound(id)) => {
                    writer.send(IpcResponse::Error {
                        code: IpcErrorCode::FolderNotFound,
                        message: format!("Folder {} not found", id),
                    }).await?;
                }
                Err(e) => {
                    writer.send(IpcResponse::Error {
                        code: IpcErrorCode::InternalError,
                        message: e.to_string(),
                    }).await?;
                }
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    supervisor: Arc<Supervisor>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (mut reader, mut writer) = framed_split(stream);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = writer.send(IpcResponse::ServerShuttingDown).await;
                break;
            }

            msg = reader.next() => {
                let req = match msg {
                    Some(Ok(req)) => req,
                    Some(Err(e)) => {
                        let _ = writer.send(IpcResponse::Error {
                            code: IpcErrorCode::InvalidRequest,
                            message: e.to_string(),
                        }).await;
                        break;
                    }
                    None => break, // EOF, client closed connection
                };

                match req {
                    IpcRequest::SubscribeTelemetry => {
                        let mut sub = supervisor.subscribe_telemetry();
                        // Push initial state
                        let initial = supervisor.get_status().await;
                        writer.send(IpcResponse::TelemetryUpdate(initial)).await?;

                        loop {
                            tokio::select! {
                                _ = cancel.cancelled() => {
                                    let _ = writer.send(IpcResponse::ServerShuttingDown).await;
                                    return Ok(());
                                }
                                update = sub.recv() => {
                                    match update {
                                        Ok(snapshot) => {
                                            if let Err(_) = writer.send(IpcResponse::TelemetryUpdate(snapshot)).await {
                                                break; // Client disconnected
                                            }
                                        }
                                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                        Err(broadcast::error::RecvError::Closed) => break,
                                    }
                                }
                                client_frame = reader.next() => {
                                    match client_frame {
                                        Some(Ok(client_req)) => {
                                            execute_request(&supervisor, &mut writer, client_req).await?;
                                        }
                                        Some(Err(e)) => {
                                            let _ = writer.send(IpcResponse::Error {
                                                code: IpcErrorCode::InvalidRequest,
                                                message: e.to_string(),
                                            }).await;
                                            break;
                                        }
                                        None => return Ok(()),
                                    }
                                }
                            }
                        }
                    }
                    other => {
                        execute_request(&supervisor, &mut writer, other).await?;
                    }
                }
            }
        }
    }

    Ok(())
}

