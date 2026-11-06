//! `sync-common` — shared types, IPC protocol, config, and state enums
//!
//! This crate is a pure library dependency of both `syncd` and `syncctl`.
//! No `std::process::exit`, no `anyhow` – all errors are typed via `thiserror`.

pub mod config;
pub mod ipc;
pub mod state;
