//! Embedded Web UI & Dashboard server for `syncctl`.
//!
//! Serves a modern, responsive, zero-dependency browser dashboard on `http://127.0.0.1:8384`
//! bridging HTTP requests to the Unix Domain Socket IPC of `syncd`.

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use sync_common::ipc::{client_framed_split, IpcRequest, IpcResponse};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
};
use tracing::info;
use uuid::Uuid;

// ─────────────────────────────────────────────
// Web Server Runner
// ─────────────────────────────────────────────

pub async fn run_web_server(socket_path: PathBuf, bind_addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("Failed to bind HTTP server to {}", bind_addr))?;

    info!("syncd Web Dashboard listening at http://{}", bind_addr);
    println!("✓ syncd Web Dashboard running at http://{}", bind_addr);
    println!("  Press Ctrl+C to stop the web server.");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let sp = socket_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_http_client(stream, sp).await {
                        tracing::debug!("HTTP connection handler error: {}", e);
                    }
                });
            }
            Err(e) => {
                tracing::warn!("TCP accept error: {}", e);
            }
        }
    }
}

// ─────────────────────────────────────────────
// HTTP Request Dispatcher
// ─────────────────────────────────────────────

async fn handle_http_client(mut stream: TcpStream, socket_path: PathBuf) -> Result<()> {
    let mut buffer = [0u8; 4096];
    let n = stream.read(&mut buffer).await?;
    if n == 0 {
        return Ok(());
    }

    let req_str = String::from_utf8_lossy(&buffer[..n]);
    let mut lines = req_str.lines();
    let request_line = match lines.next() {
        Some(l) => l,
        None => return Ok(()),
    };

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    match (method, path) {
        ("GET", "/") => {
            let html = include_str!("dashboard.html");
            send_http_response(&mut stream, 200, "text/html; charset=utf-8", html.as_bytes()).await?;
        }
        ("GET", "/api/status") => {
            match send_ipc_req(&socket_path, IpcRequest::GetStatus).await {
                Ok(IpcResponse::StatusSnapshot(snap)) => {
                    let json = serde_json::to_string(&snap)?;
                    send_http_response(&mut stream, 200, "application/json", json.as_bytes()).await?;
                }
                Ok(IpcResponse::Error { code, message }) => {
                    let err_json = format!(r#"{{"error": "{:?}", "message": "{}"}}"#, code, message);
                    send_http_response(&mut stream, 500, "application/json", err_json.as_bytes()).await?;
                }
                _ => {
                    send_http_response(&mut stream, 502, "application/json", b"{\"error\": \"IPC response error\"}").await?;
                }
            }
        }
        ("POST", "/api/pause") => {
            let body = extract_body(&req_str);
            let payload: PausePayload = serde_json::from_str(body).unwrap_or(PausePayload { folder_id: None, duration_secs: None });
            match send_ipc_req(&socket_path, IpcRequest::Pause { folder_id: payload.folder_id, duration_secs: payload.duration_secs }).await {
                Ok(IpcResponse::Ok { message }) => {
                    let resp = format!(r#"{{"ok": true, "message": "{}"}}"#, message);
                    send_http_response(&mut stream, 200, "application/json", resp.as_bytes()).await?;
                }
                _ => {
                    send_http_response(&mut stream, 500, "application/json", b"{\"error\": \"Failed to pause\"}").await?;
                }
            }
        }
        ("POST", "/api/resume") => {
            let body = extract_body(&req_str);
            let payload: FolderTargetPayload = serde_json::from_str(body).unwrap_or(FolderTargetPayload { folder_id: None });
            match send_ipc_req(&socket_path, IpcRequest::Resume { folder_id: payload.folder_id }).await {
                Ok(IpcResponse::Ok { message }) => {
                    let resp = format!(r#"{{"ok": true, "message": "{}"}}"#, message);
                    send_http_response(&mut stream, 200, "application/json", resp.as_bytes()).await?;
                }
                _ => {
                    send_http_response(&mut stream, 500, "application/json", b"{\"error\": \"Failed to resume\"}").await?;
                }
            }
        }
        ("POST", "/api/bwlimit") => {
            let body = extract_body(&req_str);
            let payload: BwLimitPayload = serde_json::from_str(body).unwrap_or(BwLimitPayload { bytes_per_sec: 0 });
            match send_ipc_req(&socket_path, IpcRequest::SetBandwidthLimit { bytes_per_sec: payload.bytes_per_sec }).await {
                Ok(IpcResponse::Ok { message }) => {
                    let resp = format!(r#"{{"ok": true, "message": "{}"}}"#, message);
                    send_http_response(&mut stream, 200, "application/json", resp.as_bytes()).await?;
                }
                _ => {
                    send_http_response(&mut stream, 500, "application/json", b"{\"error\": \"Failed to set limit\"}").await?;
                }
            }
        }
        ("POST", "/api/scan") => {
            let body = extract_body(&req_str);
            let payload: FolderTargetPayload = serde_json::from_str(body).unwrap_or(FolderTargetPayload { folder_id: None });
            match send_ipc_req(&socket_path, IpcRequest::TriggerScan { folder_id: payload.folder_id }).await {
                Ok(IpcResponse::Ok { message }) => {
                    let resp = format!(r#"{{"ok": true, "message": "{}"}}"#, message);
                    send_http_response(&mut stream, 200, "application/json", resp.as_bytes()).await?;
                }
                _ => {
                    send_http_response(&mut stream, 500, "application/json", b"{\"error\": \"Failed to scan\"}").await?;
                }
            }
        }
        _ => {
            send_http_response(&mut stream, 404, "text/plain", b"Not Found").await?;
        }
    }

    Ok(())
}

fn extract_body(req: &str) -> &str {
    if let Some(pos) = req.find("\r\n\r\n") {
        &req[pos + 4..]
    } else if let Some(pos) = req.find("\n\n") {
        &req[pos + 2..]
    } else {
        ""
    }
}

async fn send_http_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &[u8]) -> Result<()> {
    let status_line = match status {
        200 => "HTTP/1.1 200 OK",
        404 => "HTTP/1.1 404 Not Found",
        500 => "HTTP/1.1 500 Internal Server Error",
        502 => "HTTP/1.1 502 Bad Gateway",
        _ => "HTTP/1.1 200 OK",
    };

    let header = format!(
        "{}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
        status_line, content_type, body.len()
    );

    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

async fn send_ipc_req(socket_path: &PathBuf, req: IpcRequest) -> Result<IpcResponse> {
    let unix = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Cannot connect to syncd socket at '{}'", socket_path.display()))?;
    let (mut reader, mut writer) = client_framed_split(unix);
    writer.send(req).await?;
    match reader.next().await {
        Some(Ok(resp)) => Ok(resp),
        Some(Err(e)) => anyhow::bail!("IPC codec error: {}", e),
        None => anyhow::bail!("IPC connection closed"),
    }
}

// ─────────────────────────────────────────────
// Request Payloads
// ─────────────────────────────────────────────

#[derive(Deserialize)]
struct PausePayload {
    folder_id: Option<Uuid>,
    duration_secs: Option<u64>,
}

#[derive(Deserialize)]
struct FolderTargetPayload {
    folder_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct BwLimitPayload {
    bytes_per_sec: u64,
}
