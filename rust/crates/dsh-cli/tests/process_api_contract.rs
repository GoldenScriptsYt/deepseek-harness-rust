//! Focused contract checks for the browser process API.

pub mod api {
    pub use dsh_cli::api::ApiContext;
}

#[path = "../src/process_api.rs"]
mod process_api;

use dsh_cli::api::ApiContext;
use dsh_gateway::Gateway;

#[test]
fn registers_the_complete_process_surface() {
    let root = std::env::temp_dir().join(format!(
        "dsh-process-api-register-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let context = ApiContext::new("http://127.0.0.1", "test-token", &root);
    let gateway = Gateway::new("test-token", root.display().to_string(), None);
    process_api::register_all(&gateway, context);
    std::fs::remove_dir_all(root).unwrap();
}
