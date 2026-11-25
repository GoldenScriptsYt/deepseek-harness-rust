# DeepSeek Harness Rust

An unofficial community Rust Host and Tauri desktop carrier for DeepSeek Harness. This repository packages the Rust implementation separately from the upstream TypeScript/Electron monorepo.

## What is included

- rust/: Rust Host, agent, model, session, tools, workspace, and supporting crates.
- apps/desktop-tauri/: Tauri desktop shell that runs the Rust Host in-process.
- apps/web/dist/ and selected packages/*/*/lib/client.js: built web UI and Cordis client assets required by the Rust Host.

The web UI and client bundles are build artifacts from the MIT-licensed upstream project, deepseek-ai/deepseek-harness. Their source remains available in that project. The Rust port is still in progress; see REMAINING.md for known gaps.

## Build on Windows

Requirements: Rust 1.90+, Microsoft C++ Build Tools, and the WebView2 Runtime.

From the repository root:

    cargo run --locked --manifest-path apps/desktop-tauri/Cargo.toml

To assemble a portable Windows folder:

    python rust/tools/package_app.py

The package script builds the app and places the portable output under rust/dist/DeepSeekHarness/.



## License

MIT. See LICENSE and THIRD_PARTY_NOTICES.md for upstream and bundled dependency notices.
