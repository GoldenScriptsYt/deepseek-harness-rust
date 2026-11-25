# DeepSeek Harness Rust

This Tauri application runs the Rust Host in-process and serves the bundled web client.

Run from the repository root with cargo run --locked --manifest-path apps/desktop-tauri/Cargo.toml. The web client and plugin client files are committed build assets under apps/web/dist and packages/.

The Rust port is still in progress. See the repository root REMAINING.md for current feature gaps. The optional Node compatibility launcher from the upstream monorepo is intentionally not included here.
