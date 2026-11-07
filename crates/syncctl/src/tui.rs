//! Interactive Terminal User Interface (TUI) for `syncctl`.
//!
//! Provides a real-time, keyboard-driven dashboard over the Unix Domain Socket
//! that lets users monitor folders and in-flight transfers, adjust bandwidth,
//! pause with custom durations, resume, and trigger reconciliation scans.
//!
//! Architecture notes:
//! - Uses crossterm's alternate screen so the terminal is fully restored on exit.
//! - All string width calculations use `unicode_width` to handle wide glyphs (emoji).
//! - A single SubscribeTelemetry connection streams both TelemetryUpdate *and* Ok ACKs.
//! - Commands (Pause / Resume / Scan / BwLimit) are sent on the same writer; the
//!   resulting `IpcResponse::Ok` frames arrive interleaved with TelemetryUpdate and are
//!   silently consumed so the read loop is never broken by them.

use anyhow::{Context, Result};
use crossterm::{
    cursor,
    event::{Event, EventStream, KeyCode, KeyModifiers},
    execute, queue,
    style::{
        Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor,
        SetForegroundColor,
    },
    terminal::{
        disable_raw_mode, enable_raw_mode, size as terminal_size, Clear, ClearType,
        EnterAlternateScreen, LeaveAlternateScreen,
    },
};
use futures::{SinkExt, StreamExt};
use std::{
    io::{stdout, Write},
    path::PathBuf,
    time::{Duration, Instant},
};
use sync_common::{
    ipc::{client_framed_split, IpcRequest, IpcResponse},
    state::{format_bytes, FolderState, TelemetrySnapshot},
};
use tokio::net::UnixStream;
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Modal dialog state
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
enum Modal {
    None,
    Pause {
        input: String,
        folder_name: String,
        folder_id: Uuid,
    },
    BwLimit {
        input: String,
    },
    FolderDetails {
        name: String,
        id: Uuid,
        state: String,
        mode: String,
        local_path: String,
        remote_target: String,
        pending: u64,
        tracked: u64,
        safety_armed: bool,
    },
    ConfirmDeleteFolder {
        folder_id: Uuid,
        folder_name: String,
    },
    AddFolderHelp,
    Help,
}

// ─────────────────────────────────────────────────────────────────────────────
// RAII Terminal Guard — restores the terminal even on panic
// ─────────────────────────────────────────────────────────────────────────────

struct TerminalGuard;

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen, cursor::Hide)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(stdout(), cursor::Show, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Flash message helper
// ─────────────────────────────────────────────────────────────────────────────

type FlashMsg = (String, Instant, Color);

fn flash(msg: impl Into<String>, color: Color) -> Option<FlashMsg> {
    Some((msg.into(), Instant::now(), color))
}

fn flash_expired(f: &FlashMsg) -> bool {
    f.1.elapsed() >= Duration::from_secs(4)
}

// ─────────────────────────────────────────────────────────────────────────────
// Main TUI event loop
// ─────────────────────────────────────────────────────────────────────────────

pub async fn run_tui(socket_path: PathBuf) -> Result<()> {
    let _guard = TerminalGuard::new()?;

    let stream = UnixStream::connect(&socket_path)
        .await
        .with_context(|| format!("Could not connect to syncd at '{}'", socket_path.display()))?;

    let (mut reader, mut writer) = client_framed_split(stream);

    // Subscribe to streaming telemetry; all subsequent frames come back on this
    // connection — both TelemetryUpdate and Ok ACKs for commands we send.
    writer.send(IpcRequest::SubscribeTelemetry).await?;

    let mut event_stream = EventStream::new();
    let mut snapshot: Option<TelemetrySnapshot> = None;
    let mut selected_idx: usize = 0;
    let mut modal = Modal::None;
    let mut flash_msg: Option<FlashMsg> = None;

    loop {
        // Clear expired flash messages before drawing
        if let Some(ref fm) = flash_msg {
            if flash_expired(fm) {
                flash_msg = None;
            }
        }

        draw_ui(snapshot.as_ref(), selected_idx, &modal, flash_msg.as_ref())?;

        tokio::select! {
            // ── Daemon telemetry / ACK stream ────────────────────────────────
            msg = reader.next() => {
                match msg {
                    Some(Ok(IpcResponse::TelemetryUpdate(snap))) => {
                        let count = snap.folders.len();
                        if count > 0 && selected_idx >= count {
                            selected_idx = count - 1;
                        }
                        snapshot = Some(snap);
                    }
                    Some(Ok(IpcResponse::Ok { .. })) => {
                        // ACK from Pause / Resume / Scan / BwLimit; consumed silently.
                    }
                    Some(Ok(IpcResponse::ServerShuttingDown)) => {
                        flash_msg = flash("syncd daemon is shutting down…", Color::Yellow);
                        draw_ui(snapshot.as_ref(), selected_idx, &modal, flash_msg.as_ref())?;
                        tokio::time::sleep(Duration::from_millis(1200)).await;
                        break;
                    }
                    Some(Err(e)) => {
                        flash_msg = flash(format!("IPC read error: {}", e), Color::Red);
                    }
                    None => {
                        flash_msg = flash("Connection lost to syncd", Color::Red);
                        draw_ui(snapshot.as_ref(), selected_idx, &modal, flash_msg.as_ref())?;
                        tokio::time::sleep(Duration::from_millis(1200)).await;
                        break;
                    }
                    _ => {}
                }
            }

            // ── Keyboard / terminal events ───────────────────────────────────
            Some(Ok(event)) = event_stream.next() => {
                // Terminal resize: just redraw on the next loop iteration
                if let Event::Resize(_, _) = event {
                    continue;
                }

                let Event::Key(key) = event else { continue };

                // Global: Ctrl-C always quits
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    break;
                }

                match &mut modal {
                    // ── Normal (no modal open) ──────────────────────────────
                    Modal::None => {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => break,

                            KeyCode::Up | KeyCode::Char('k') => {
                                selected_idx = selected_idx.saturating_sub(1);
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                if let Some(snap) = &snapshot {
                                    if selected_idx + 1 < snap.folders.len() {
                                        selected_idx += 1;
                                    }
                                }
                            }

                            KeyCode::Enter | KeyCode::Char('i') | KeyCode::Char('I') => {
                                if let Some(snap) = &snapshot {
                                    if let Some(f) = snap.folders.get(selected_idx) {
                                        let (state_str, _) = folder_state_display(&f.state);
                                        modal = Modal::FolderDetails {
                                            name: f.name.clone(),
                                            id: f.folder_id,
                                            state: state_str,
                                            mode: f.sync_mode.clone(),
                                            local_path: f.local_path.clone(),
                                            remote_target: f.remote_target.clone(),
                                            pending: f.pending_tasks,
                                            tracked: f.baseline_file_count,
                                            safety_armed: f.safety_lock_armed,
                                        };
                                    }
                                }
                            }

                            KeyCode::Char('p') | KeyCode::Char('P') => {
                                if let Some(snap) = &snapshot {
                                    if let Some(f) = snap.folders.get(selected_idx) {
                                        if f.state.is_paused() {
                                            // Already paused → resume immediately
                                            let fid = f.folder_id;
                                            let name = f.name.clone();
                                            writer.send(IpcRequest::Resume { folder_id: Some(fid) }).await?;
                                            flash_msg = flash(format!("▶ Resumed '{}'", name), Color::Green);
                                        } else {
                                            modal = Modal::Pause {
                                                input: String::new(),
                                                folder_name: f.name.clone(),
                                                folder_id: f.folder_id,
                                            };
                                        }
                                    } else {
                                        flash_msg = flash("No folder selected", Color::DarkYellow);
                                    }
                                }
                            }

                            KeyCode::Char('r') | KeyCode::Char('R') => {
                                if let Some(snap) = &snapshot {
                                    if let Some(f) = snap.folders.get(selected_idx) {
                                        let fid = f.folder_id;
                                        let name = f.name.clone();
                                        writer.send(IpcRequest::Resume { folder_id: Some(fid) }).await?;
                                        flash_msg = flash(format!("▶ Resumed '{}'", name), Color::Green);
                                    }
                                }
                            }

                            KeyCode::Char('b') | KeyCode::Char('B') => {
                                modal = Modal::BwLimit { input: String::new() };
                            }

                            KeyCode::Char('s') | KeyCode::Char('S') => {
                                if let Some(snap) = &snapshot {
                                    if let Some(f) = snap.folders.get(selected_idx) {
                                        let fid = f.folder_id;
                                        let name = f.name.clone();
                                        writer.send(IpcRequest::TriggerScan { folder_id: Some(fid) }).await?;
                                        flash_msg = flash(format!("🔍 Triggered scan for '{}'", name), Color::Cyan);
                                    }
                                }
                            }

                            // Quick rate-limit presets
                            KeyCode::Char('0') => {
                                writer.send(IpcRequest::SetBandwidthLimit { bytes_per_sec: 0 }).await?;
                                flash_msg = flash("Bandwidth → Unlimited", Color::Green);
                            }
                            KeyCode::Char('1') => {
                                writer.send(IpcRequest::SetBandwidthLimit { bytes_per_sec: 1024 * 1024 }).await?;
                                flash_msg = flash("Bandwidth → 1 MiB/s", Color::Cyan);
                            }
                            KeyCode::Char('5') => {
                                writer.send(IpcRequest::SetBandwidthLimit { bytes_per_sec: 5 * 1024 * 1024 }).await?;
                                flash_msg = flash("Bandwidth → 5 MiB/s", Color::Cyan);
                            }

                            KeyCode::Char('d') | KeyCode::Char('D') | KeyCode::Char('x') | KeyCode::Char('X') => {
                                if let Some(snap) = &snapshot {
                                    if let Some(f) = snap.folders.get(selected_idx) {
                                        modal = Modal::ConfirmDeleteFolder {
                                            folder_id: f.folder_id,
                                            folder_name: f.name.clone(),
                                        };
                                    } else {
                                        flash_msg = flash("No folder selected", Color::DarkYellow);
                                    }
                                }
                            }

                            KeyCode::Char('a') | KeyCode::Char('A') => {
                                modal = Modal::AddFolderHelp;
                            }

                            KeyCode::Char('?') => {
                                modal = Modal::Help;
                            }

                            _ => {}
                        }
                    }

                    // ── Pause dialog ────────────────────────────────────────
                    Modal::Pause { input, folder_id, folder_name } => {
                        match key.code {
                            KeyCode::Esc => {
                                modal = Modal::None;
                            }
                            KeyCode::Backspace => {
                                input.pop();
                            }
                            KeyCode::Enter => {
                                // Empty input → pause indefinitely
                                let duration_secs = if input.trim().is_empty() {
                                    None
                                } else {
                                    match crate::parse_duration(input) {
                                        Ok(0) => {
                                            flash_msg = flash("Duration must be > 0 (use blank for indefinite)", Color::Red);
                                            modal = Modal::None;
                                            continue;
                                        }
                                        Ok(s) => Some(s),
                                        Err(e) => {
                                            flash_msg = flash(format!("Invalid duration '{}': {}", input, e), Color::Red);
                                            modal = Modal::None;
                                            continue;
                                        }
                                    }
                                };
                                let fid = *folder_id;
                                let name = folder_name.clone();
                                writer.send(IpcRequest::Pause { folder_id: Some(fid), duration_secs }).await?;
                                let desc = format_duration_desc(duration_secs);
                                flash_msg = flash(format!("⏸ Paused '{}' {}", name, desc), Color::Yellow);
                                modal = Modal::None;
                            }
                            KeyCode::Char(c) if !c.is_control() => {
                                input.push(c);
                            }
                            _ => {}
                        }
                    }

                    // ── Bandwidth limit dialog ──────────────────────────────
                    Modal::BwLimit { input } => {
                        match key.code {
                            KeyCode::Esc => {
                                modal = Modal::None;
                            }
                            KeyCode::Backspace => {
                                input.pop();
                            }
                            KeyCode::Enter => {
                                let rate = if input.trim().is_empty() {
                                    0u64 // empty → unlimited
                                } else {
                                    match crate::parse_bandwidth(input) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            flash_msg = flash(format!("Invalid rate '{}': {}", input, e), Color::Red);
                                            modal = Modal::None;
                                            continue;
                                        }
                                    }
                                };
                                writer.send(IpcRequest::SetBandwidthLimit { bytes_per_sec: rate }).await?;
                                flash_msg = flash(
                                    if rate == 0 {
                                        "Bandwidth → Unlimited".into()
                                    } else {
                                        format!("Bandwidth → {}/s cap", format_bytes(rate))
                                    },
                                    Color::Green,
                                );
                                modal = Modal::None;
                            }
                            KeyCode::Char(c) if !c.is_control() => {
                                input.push(c);
                            }
                            _ => {}
                        }
                    }

                    // ── Folder details modal ────────────────────────────────
                    Modal::FolderDetails { .. } => {
                        // Any key dismisses folder details
                        modal = Modal::None;
                    }

                    // ── Confirm delete folder modal ─────────────────────────
                    Modal::ConfirmDeleteFolder { folder_id, folder_name } => {
                        match key.code {
                            KeyCode::Esc => {
                                modal = Modal::None;
                            }
                            KeyCode::Enter => {
                                let fid = *folder_id;
                                let name = folder_name.clone();
                                modal = Modal::None;

                                let config_path = sync_common::config::GlobalConfig::default_config_path();
                                match sync_common::config::GlobalConfig::from_file(&config_path) {
                                    Ok(mut cfg) => {
                                        cfg.folders.retain(|f| f.id != fid);
                                        if let Err(e) = cfg.save_to_file(&config_path) {
                                            flash_msg = flash(format!("Failed saving config: {}", e), Color::Red);
                                        } else {
                                            let _ = writer.send(IpcRequest::ReloadConfig).await;
                                            flash_msg = flash(format!("✓ Removed folder '{}'", name), Color::Green);
                                        }
                                    }
                                    Err(e) => {
                                        flash_msg = flash(format!("Failed reading config: {}", e), Color::Red);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    // ── Add folder info modal ───────────────────────────────
                    Modal::AddFolderHelp => {
                        modal = Modal::None;
                    }

                    // ── Help overlay ────────────────────────────────────────
                    Modal::Help => {
                        // Any key dismisses help
                        modal = Modal::None;
                    }
                }
            }
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Duration formatting helper
// ─────────────────────────────────────────────────────────────────────────────

fn format_duration_desc(secs: Option<u64>) -> String {
    match secs {
        None => "indefinitely".into(),
        Some(s) if s >= 3600 => format!("for {:.1}h", s as f64 / 3600.0),
        Some(s) if s >= 60 => format!("for {}m", s / 60),
        Some(s) => format!("for {}s", s),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UI drawing
// ─────────────────────────────────────────────────────────────────────────────

fn draw_ui(
    snap_opt: Option<&TelemetrySnapshot>,
    selected_idx: usize,
    modal: &Modal,
    flash_msg: Option<&FlashMsg>,
) -> Result<()> {
    let mut out = stdout();
    let (term_w, term_h) = terminal_size().unwrap_or((100, 30));
    let w = term_w as usize;
    // Reserve 1 column to prevent wrapping at the exact last column
    let max_w = w.saturating_sub(1);
    let footer_y = term_h.saturating_sub(1);
    // All content must stay at or above this row
    let content_bottom = footer_y.saturating_sub(1);

    if term_h < 8 {
        queue!(
            out,
            cursor::MoveTo(0, 0),
            Clear(ClearType::All),
            Print("Terminal too small — please resize to ≥ 8 rows"),
        )?;
        out.flush()?;
        return Ok(());
    }

    let mut row: u16 = 0;

    // ── 1. Header banner ──────────────────────────────────────────────────────
    let (conn_label, _conn_color) = if snap_opt.is_some() {
        ("● Connected", Color::Green)
    } else {
        ("○ Connecting…", Color::Yellow)
    };
    let speed_str = snap_opt
        .map(|s| format!("{}/s", format_bytes(s.global_upload_bps)))
        .unwrap_or_else(|| "—".into());
    let bw_cap_str = snap_opt
        .map(|s| {
            if s.configured_bw_limit_bps == 0 {
                "Unlimited".into()
            } else {
                format!("{}/s", format_bytes(s.configured_bw_limit_bps))
            }
        })
        .unwrap_or_else(|| "—".into());
    let n_transfers = snap_opt.map_or(0, |s| s.active_transfers.len());
    let n_pending: u64 = snap_opt.map_or(0, |s| s.folders.iter().map(|f| f.pending_tasks).sum());

    let header = format!(
        "  syncd  │  ↑ {}  │  Limit: {}  │  Transfers: {}  │  Queued: {}  │  {}  ",
        speed_str, bw_cap_str, n_transfers, n_pending, conn_label
    );
    queue!(
        out,
        cursor::MoveTo(0, row),
        SetBackgroundColor(Color::DarkBlue),
        SetForegroundColor(Color::White),
        SetAttribute(Attribute::Bold),
        Print(display_pad(&header, max_w)),
        ResetColor,
        Clear(ClearType::UntilNewLine),
    )?;
    // Colour-tint the connection label separately
    // (already embedded in the string above; just reset colour)
    row += 1;

    // ── 2. Flash message row ──────────────────────────────────────────────────
    if row <= content_bottom {
        match flash_msg {
            Some(fm) if !flash_expired(fm) => {
                queue!(
                    out,
                    cursor::MoveTo(0, row),
                    SetForegroundColor(fm.2),
                    SetAttribute(Attribute::Bold),
                    Print(display_pad(&format!("  ▶ {}", fm.0), max_w)),
                    ResetColor,
                    Clear(ClearType::UntilNewLine),
                )?;
            }
            _ => {
                queue!(out, cursor::MoveTo(0, row), Clear(ClearType::UntilNewLine))?;
            }
        }
        row += 1;
    }

    // ── 3. WATCHED FOLDERS section ────────────────────────────────────────────
    if row <= content_bottom {
        queue!(
            out,
            cursor::MoveTo(0, row),
            SetAttribute(Attribute::Bold),
            SetForegroundColor(Color::Cyan),
            Print(display_pad("  WATCHED FOLDERS", max_w)),
            ResetColor,
            Clear(ClearType::UntilNewLine),
        )?;
        row += 1;
    }

    // Column widths (in terminal display columns, not bytes or codepoints)
    const NAME_W: usize = 20;
    const STATE_W: usize = 18;
    const MODE_W: usize = 14;
    const PEND_W: usize = 7;
    const SYNCED_W: usize = 7;
    const PATH_W: usize = 36;

    // Table header row
    if row <= content_bottom {
        let hdr = format!(
            "   {:<name$} {:<state$} {:<mode$} {:<pend$} {:<tracked$} {}",
            "Name", "State", "Mode", "Pending", "Tracked", "Local Path",
            name = NAME_W, state = STATE_W, mode = MODE_W, pend = PEND_W, tracked = SYNCED_W,
        );
        queue!(
            out,
            cursor::MoveTo(0, row),
            SetAttribute(Attribute::Underlined),
            SetForegroundColor(Color::DarkGrey),
            Print(display_pad(&hdr, max_w)),
            ResetColor,
            Clear(ClearType::UntilNewLine),
        )?;
        row += 1;
    }

    if let Some(snap) = snap_opt {
        if snap.folders.is_empty() {
            if row <= content_bottom {
                queue!(
                    out,
                    cursor::MoveTo(0, row),
                    SetForegroundColor(Color::DarkGrey),
                    Print(display_pad("   (No folders configured in ~/.config/syncd/config.toml)", max_w)),
                    ResetColor,
                    Clear(ClearType::UntilNewLine),
                )?;
                row += 1;
            }
        } else {
            for (idx, f) in snap.folders.iter().enumerate() {
                if row > content_bottom {
                    break;
                }

                let is_selected = idx == selected_idx;
                let (state_str, state_color) = folder_state_display(&f.state);
                let marker = if is_selected { "▶" } else { " " };

                // Build each field with display-width-aware padding
                let name_col = display_truncate_pad(&f.name, NAME_W);
                let state_col = display_truncate_pad(&state_str, STATE_W);
                let mode_badge = match f.sync_mode.as_str() {
                    "continuous_backup" => {
                        if f.safety_lock_armed {
                            "[BACKUP:SAFE]"
                        } else {
                            "[BACKUP]"
                        }
                    }
                    "drop_and_purge" => "[OFFLOAD]",
                    other => other,
                };
                let mode_col = display_truncate_pad(mode_badge, MODE_W);
                let pend_col = display_truncate_pad(&format!("{}", f.pending_tasks), PEND_W);
                let synced_col = display_truncate_pad(&format!("{}", f.baseline_file_count), SYNCED_W);
                let path_col = display_truncate_pad(&f.local_path, PATH_W);

                let line = format!(
                    " {} {} {} {} {} {} {}",
                    marker, name_col, state_col, mode_col, pend_col, synced_col, path_col
                );

                if is_selected {
                    queue!(
                        out,
                        cursor::MoveTo(0, row),
                        SetBackgroundColor(Color::DarkGrey),
                        SetForegroundColor(Color::White),
                        SetAttribute(Attribute::Bold),
                        Print(display_pad(&line, max_w)),
                        ResetColor,
                        Clear(ClearType::UntilNewLine),
                    )?;
                } else {
                    queue!(
                        out,
                        cursor::MoveTo(0, row),
                        SetForegroundColor(state_color),
                        Print(display_pad(&line, max_w)),
                        ResetColor,
                        Clear(ClearType::UntilNewLine),
                    )?;
                }
                row += 1;
            }
        }
    } else if row <= content_bottom {
        queue!(
            out,
            cursor::MoveTo(0, row),
            SetForegroundColor(Color::DarkGrey),
            Print(display_pad("   Connecting to syncd…", max_w)),
            ResetColor,
            Clear(ClearType::UntilNewLine),
        )?;
        row += 1;
    }

    // ── 4. Spacer ─────────────────────────────────────────────────────────────
    if row <= content_bottom {
        queue!(out, cursor::MoveTo(0, row), Clear(ClearType::UntilNewLine))?;
        row += 1;
    }

    // ── 5. ACTIVE IN-FLIGHT TRANSFERS section ─────────────────────────────────
    if row <= content_bottom {
        queue!(
            out,
            cursor::MoveTo(0, row),
            SetAttribute(Attribute::Bold),
            SetForegroundColor(Color::Cyan),
            Print(display_pad("  ACTIVE IN-FLIGHT TRANSFERS", max_w)),
            ResetColor,
            Clear(ClearType::UntilNewLine),
        )?;
        row += 1;
    }

    if let Some(snap) = snap_opt {
        if snap.active_transfers.is_empty() {
            if row <= content_bottom {
                queue!(
                    out,
                    cursor::MoveTo(0, row),
                    SetForegroundColor(Color::DarkGrey),
                    Print(display_pad("   (Idle — no active file transfers in progress)", max_w)),
                    ResetColor,
                    Clear(ClearType::UntilNewLine),
                )?;
                row += 1;
            }
        } else {
            // Transfers header
            if row <= content_bottom {
                let t_hdr = format!(
                    "   {:<8} {:<10} {:<28} {:<20} {:<12} {:<8}",
                    "Task", "Op", "File", "Progress", "Speed", "ETA"
                );
                queue!(
                    out,
                    cursor::MoveTo(0, row),
                    SetAttribute(Attribute::Underlined),
                    SetForegroundColor(Color::DarkGrey),
                    Print(display_pad(&t_hdr, max_w)),
                    ResetColor,
                    Clear(ClearType::UntilNewLine),
                )?;
                row += 1;
            }

            // Build a folder-name lookup from snapshot folders
            let folder_names: std::collections::HashMap<Uuid, &str> = snap
                .folders
                .iter()
                .map(|f| (f.folder_id, f.name.as_str()))
                .collect();

            for t in &snap.active_transfers {
                if row > content_bottom {
                    break;
                }
                let pct = if t.total_bytes > 0 {
                    (t.bytes_transferred as f64 / t.total_bytes as f64).clamp(0.0, 1.0)
                } else {
                    1.0 // zero-byte file is "complete"
                };
                let bar = make_progress_bar(pct, 10);
                let progress_str = format!("{} {:>3.0}%", bar, pct * 100.0);
                let speed_str = format!("{}/s", format_bytes(t.upload_bps_ewma.round() as u64));
                let eta_str = match t.eta_secs {
                    Some(s) if s < 60.0 => format!("{:.0}s", s),
                    Some(s) => format!("{:.1}m", s / 60.0),
                    None => "—".into(),
                };
                let folder_prefix = folder_names
                    .get(&t.folder_id)
                    .copied()
                    .unwrap_or("?")
                    .chars()
                    .take(4)
                    .collect::<String>();
                let task_col = format!("{}({})", folder_prefix, t.task_id);

                let line = format!(
                    "   {:<8} {:<10} {:<28} {:<20} {:<12} {:<8}",
                    display_truncate(&task_col, 8),
                    t.op_type.as_db_str(),
                    display_truncate(&t.rel_path, 28),
                    display_truncate(&progress_str, 20),
                    display_truncate(&speed_str, 12),
                    eta_str,
                );
                queue!(
                    out,
                    cursor::MoveTo(0, row),
                    SetForegroundColor(Color::White),
                    Print(display_pad(&line, max_w)),
                    ResetColor,
                    Clear(ClearType::UntilNewLine),
                )?;
                row += 1;
            }
        }
    }

    // ── 6. Clear remaining content rows above the footer ──────────────────────
    while row <= content_bottom {
        queue!(out, cursor::MoveTo(0, row), Clear(ClearType::UntilNewLine))?;
        row += 1;
    }

    // ── 7. Modal dialog overlay ───────────────────────────────────────────────
    match modal {
        Modal::Pause { input, folder_name, .. } => {
            draw_dialog(
                &mut out,
                term_w,
                term_h,
                "⏸  Pause Folder",
                &[
                    &format!("  Folder: {}", folder_name),
                    "  Duration  (e.g. 30s  15m  2h  or leave blank for indefinite):",
                    &format!("  > {}█", input),
                    "",
                    "  [Enter] Confirm      [Esc] Cancel",
                ],
            )?;
        }
        Modal::BwLimit { input } => {
            draw_dialog(
                &mut out,
                term_w,
                term_h,
                "📶  Set Upload Bandwidth Limit",
                &[
                    "  Enter cap  (e.g. 10MB/s  500KB  or blank / 0 for unlimited):",
                    &format!("  > {}█", input),
                    "",
                    "  Presets:  [0] Unlimited   [1] 1 MiB/s   [5] 5 MiB/s",
                    "  [Enter] Confirm      [Esc] Cancel",
                ],
            )?;
        }
        Modal::FolderDetails {
            name,
            id,
            state,
            mode,
            local_path,
            remote_target,
            pending,
            tracked,
            safety_armed,
        } => {
            let safety_desc = if *safety_armed {
                "Armed (blocks deletes if folder emptied)"
            } else {
                "Disabled"
            };
            let mode_desc = match mode.as_str() {
                "continuous_backup" => "Continuous Backup (preserves cloud trash)",
                "drop_and_purge" => "Drop & Purge (offload file after verify)",
                other => other,
            };
            let id_str = id.to_string();
            let pend_str = format!("{} task(s) in transfer queue", pending);
            let track_str = format!("{} file(s) tracked (safety baseline)", tracked);
            draw_dialog(
                &mut out,
                term_w,
                term_h,
                &format!("ℹ  Folder Properties: {}", name),
                &[
                    &format!("  Folder ID:     {}", id_str),
                    &format!("  Current State: {}", state),
                    &format!("  Sync Mode:     {} [Safety: {}]", mode_desc, safety_desc),
                    &format!("  Local Path:    {}", local_path),
                    &format!("  Remote Target: {}", remote_target),
                    &format!("  Tracked Files: {}", track_str),
                    &format!("  Pending Tasks: {}", pend_str),
                    "",
                    "  [Enter / Esc] Close",
                ],
            )?;
        }
        Modal::ConfirmDeleteFolder { folder_name, .. } => {
            draw_dialog(
                &mut out,
                term_w,
                term_h,
                "⚠  Confirm Folder Removal",
                &[
                    &format!("  Are you sure you want to remove folder '{}'?", folder_name),
                    "  This stops watching and offloading for this folder.",
                    "  (Local and cloud files will remain untouched)",
                    "",
                    "  [Enter] Confirm & Remove   [Esc] Cancel",
                ],
            )?;
        }
        Modal::AddFolderHelp => {
            draw_dialog(
                &mut out,
                term_w,
                term_h,
                "+  Add Watched Folder",
                &[
                    "  To add a folder to syncd, exit the TUI and run:",
                    "",
                    "    syncctl folder add",
                    "",
                    "  or launch the interactive console:",
                    "",
                    "    syncctl folder",
                    "",
                    "  [Enter / Esc] Close",
                ],
            )?;
        }
        Modal::Help => {
            draw_dialog(
                &mut out,
                term_w,
                term_h,
                "?  Keyboard Reference",
                &[
                    "  Up / k      Select folder up",
                    "  Down / j    Select folder down",
                    "  Enter / i   View selected folder properties & details",
                    "  P           Pause selected folder (or Resume if already paused)",
                    "  R           Resume selected folder",
                    "  A           How to add a new folder",
                    "  D / x       Delete / remove selected folder from configuration",
                    "  B           Open bandwidth-limit dialog",
                    "  S           Trigger reconciliation scan",
                    "  0           Remove bandwidth cap (Unlimited)",
                    "  1           Cap at 1 MiB/s",
                    "  5           Cap at 5 MiB/s",
                    "  ?           Show this help overlay",
                    "  Q / Esc     Quit",
                ],
            )?;
        }
        Modal::None => {}
    }

    // ── 8. Footer controls bar (always at the very last row) ──────────────────
    let pause_hint = snap_opt
        .and_then(|s| s.folders.get(selected_idx))
        .map(|f| if f.state.is_paused() { "[P] Resume" } else { "[P] Pause" })
        .unwrap_or("[P] Pause");

    let footer = format!(
        " [↑/↓] Select  •  [Enter/i] Details  •  {}  •  [A] Add  •  [D] Delete  •  [B] Bw  •  [S] Scan  •  [?] Help  •  [Q] Quit",
        pause_hint
    );
    queue!(
        out,
        cursor::MoveTo(0, footer_y),
        SetBackgroundColor(Color::DarkGrey),
        SetForegroundColor(Color::White),
        Print(display_pad(&footer, max_w)),
        ResetColor,
        Clear(ClearType::UntilNewLine),
    )?;

    out.flush()?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// State display helper
// ─────────────────────────────────────────────────────────────────────────────

pub fn folder_state_display(state: &FolderState) -> (String, Color) {
    match state {
        FolderState::Watching => ("Watching".into(), Color::Green),
        FolderState::Syncing => ("Syncing (upload)".into(), Color::Cyan),
        FolderState::Debouncing => ("Debouncing...".into(), Color::Blue),
        FolderState::Paused { until: None } => ("Paused (indefinite)".into(), Color::Yellow),
        FolderState::Paused { until: Some(ts) } => (
            format!("Paused till {:}", ts.format("%H:%M")),
            Color::Yellow,
        ),
        FolderState::DriveDisconnected => ("Disconnected".into(), Color::Red),
        FolderState::Error { reason } => (
            format!("Error: {}", display_truncate(reason, 14)),
            Color::Red,
        ),
        FolderState::Reconciling => ("Reconciling...".into(), Color::Magenta),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// String / display-width helpers (unicode_width-aware)
// ─────────────────────────────────────────────────────────────────────────────

/// Truncate `s` so its terminal display width is ≤ `max_cols`.
/// Appends `…` when the string is cut.
fn display_truncate(s: &str, max_cols: usize) -> String {
    let w = s.width();
    if w <= max_cols {
        return s.to_string();
    }
    // Need to find the cut point in terms of display width
    let mut acc = 0usize;
    let mut cut = s.len();
    for (i, c) in s.char_indices() {
        let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if acc + cw > max_cols.saturating_sub(1) {
            cut = i;
            break;
        }
        acc += cw;
    }
    format!("{}…", &s[..cut])
}

/// Truncate then right-pad `s` with spaces so its display width is exactly `width` columns.
fn display_truncate_pad(s: &str, width: usize) -> String {
    let truncated = display_truncate(s, width);
    let tw = truncated.width();
    let padding = width.saturating_sub(tw);
    format!("{}{}", truncated, " ".repeat(padding))
}

/// Pad (or truncate) `s` to exactly `width` display columns.
fn display_pad(s: &str, width: usize) -> String {
    display_truncate_pad(s, width)
}

// ─────────────────────────────────────────────────────────────────────────────
// Progress bar
// ─────────────────────────────────────────────────────────────────────────────

fn make_progress_bar(fraction: f64, bar_width: usize) -> String {
    let filled = ((fraction * bar_width as f64).round() as usize).min(bar_width);
    let empty = bar_width - filled;
    // Use ASCII block characters to avoid wide-char width surprises
    format!("[{}{}]", "#".repeat(filled), "-".repeat(empty))
}

// ─────────────────────────────────────────────────────────────────────────────
// Modal dialog renderer
// ─────────────────────────────────────────────────────────────────────────────

fn draw_dialog(
    out: &mut std::io::Stdout,
    term_w: u16,
    term_h: u16,
    title: &str,
    lines: &[&str],
) -> Result<()> {
    // Inner usable width inside the box borders (1 space padding each side)
    let inner_w: usize = 64;
    let box_w = (inner_w + 4) as u16; // ║ <space> … <space> ║
    let box_h = (lines.len() as u16) + 4; // top border + blank + lines + bottom border

    let start_x = (term_w.saturating_sub(box_w)) / 2;
    let start_y = (term_h.saturating_sub(box_h)) / 2;

    // Top border with title
    let title_disp = display_truncate(title, inner_w.saturating_sub(4));
    let title_dw = title_disp.width();
    let fill = inner_w.saturating_sub(title_dw + 3); // "═ " + title + " " + fill + "═"
    queue!(
        out,
        cursor::MoveTo(start_x, start_y),
        SetBackgroundColor(Color::Blue),
        SetForegroundColor(Color::White),
        SetAttribute(Attribute::Bold),
        Print(format!("╔═ {}{} ╗", title_disp, "═".repeat(fill))),
        ResetColor,
    )?;

    // Blank row after title
    queue!(
        out,
        cursor::MoveTo(start_x, start_y + 1),
        SetBackgroundColor(Color::Blue),
        SetForegroundColor(Color::White),
        Print(format!("║ {:width$} ║", "", width = inner_w + 1)),
        ResetColor,
    )?;

    // Content lines
    for (i, line) in lines.iter().enumerate() {
        let display = display_truncate(line, inner_w);
        let dw = display.width();
        let pad = inner_w.saturating_sub(dw);
        queue!(
            out,
            cursor::MoveTo(start_x, start_y + 2 + i as u16),
            SetBackgroundColor(Color::Blue),
            SetForegroundColor(Color::White),
            Print(format!("║ {}{} ║", display, " ".repeat(pad))),
            ResetColor,
        )?;
    }

    // Bottom border
    queue!(
        out,
        cursor::MoveTo(start_x, start_y + box_h - 1),
        SetBackgroundColor(Color::Blue),
        SetForegroundColor(Color::White),
        Print(format!("╚{}╝", "═".repeat(inner_w + 2))),
        ResetColor,
    )?;

    Ok(())
}
