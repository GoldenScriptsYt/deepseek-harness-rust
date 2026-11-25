use dsh_cli::{run, web, Args};
use dsh_core::Cancel;
use dsh_llm_deepseek::client::{Connection, PUBLIC_BASE_URL};

async fn serve_web(argv: &[String]) -> i32 {
    let mut port = 3080u16;
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--port" => port = it.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--no-open" => {}
            other => {
                eprintln!("unknown option {other}");
                return 2;
            }
        }
    }
    let Some(repo) = web::locate_resources() else {
        eprintln!("cannot find the web client (apps/web/dist); set DSH_REPO_ROOT");
        return 1;
    };
    let started = match web::start(&repo, &web::default_home(), port).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot start the web Host on 127.0.0.1:{port}: {e}");
            return 1;
        }
    };
    println!("dsh web: {}", started.entry_url);
    match started.serve.await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("server stopped: {e}");
            1
        }
    }
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.first().map(String::as_str) == Some("web") {
        std::process::exit(serve_web(&argv[1..]).await);
    }
    let args = match Args::parse(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let Ok(key) = std::env::var("DEEPSEEK_API_KEY") else {
        eprintln!("DEEPSEEK_API_KEY is not set");
        std::process::exit(2);
    };
    let mut conn = Connection::new(key);
    conn.base_url = std::env::var("DEEPSEEK_BASE_URL").unwrap_or_else(|_| PUBLIC_BASE_URL.into());

    let cancel = Cancel::default();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            c2.cancel();
        }
    });
    let mut out = std::io::stdout();
    match run(args, conn, cancel, &mut out).await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
