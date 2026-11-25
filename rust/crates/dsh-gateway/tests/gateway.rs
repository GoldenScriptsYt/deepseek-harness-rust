use dsh_gateway::{Gateway, RpcError};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type MuxSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start() -> (String, Gateway) {
    let dir = std::env::temp_dir().join(format!("dsh-gw-static-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(dir.join("index.html"), "<html>hi</html>").unwrap();
    std::fs::write(dir.join("assets/a.js"), "console.log(1)").unwrap();
    let gw = Gateway::new("launch-token", "/home/me", Some(dir));
    gw.rpc("permissionPresets/catalog", |_| async {
        Ok(json!({"options":[{"value":"read-only"}]}))
    });
    gw.rpc("session/fail", |_| async {
        Err(RpcError::new("session/not-found", "no such session"))
    });
    gw.stream("count", |payload, mut uplink| {
        let n = payload["args"]["n"].as_u64().unwrap_or(0);
        Box::pin(async_stream(n, async move { uplink.recv().await }))
    });
    let (addr, serve) = gw.bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(serve);
    (format!("127.0.0.1:{}", addr.port()), gw)
}

/// Emit 0..n, then echo one uplink item if the client sends it.
fn async_stream(
    n: u64,
    first_uplink: impl std::future::Future<Output = Option<Value>> + Send + 'static,
) -> impl futures_util::Stream<Item = Result<Value, RpcError>> + Send + 'static {
    let items: Vec<Result<Value, RpcError>> = (0..n).map(|i| Ok(json!(i))).collect();
    futures_util::stream::iter(items).chain(futures_util::stream::once(async move {
        Ok(json!({"echo": first_uplink.await.unwrap_or(Value::Null)}))
    }))
}

async fn sign_in(addr: &str) -> String {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let bad = client
        .get(format!("http://{addr}/?token=wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401);
    let ok = client
        .get(format!("http://{addr}/?token=launch-token"))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 303);
    assert_eq!(ok.headers()["location"], "./");
    let set = ok.headers()["set-cookie"].to_str().unwrap().to_string();
    assert!(set.contains("HttpOnly") && set.contains("SameSite=Strict"));
    set.split(';').next().unwrap().to_string()
}

fn request(id: &str, method: &str, payload: Value) -> String {
    json!({"type":"client-request","rpcId":id,"method":method,"payload":payload}).to_string()
}

#[tokio::test]
async fn auth_static_and_rpc() {
    let (addr, _gw) = start().await;
    let client = reqwest::Client::new();

    // Everything but the sign-in exchange needs the cookie.
    assert_eq!(
        client
            .get(format!("http://{addr}/"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("http://{addr}/api/permissionPresets/catalog"))
            .body("{}")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    let cookie = sign_in(&addr).await;
    let get = |path: &str| {
        client
            .get(format!("http://{addr}{path}"))
            .header("cookie", &cookie)
            .send()
    };
    assert_eq!(
        get("/").await.unwrap().text().await.unwrap(),
        "<html>hi</html>"
    );
    let js = get("/assets/a.js").await.unwrap();
    assert_eq!(
        js.headers()["content-type"],
        "text/javascript; charset=utf-8"
    );
    assert_eq!(get("/assets/missing.js").await.unwrap().status(), 404);

    let call = |method: &str, body: String| {
        client
            .post(format!("http://{addr}/api/{method}"))
            .header("cookie", &cookie)
            .body(body)
            .send()
    };
    let ok: Value = call(
        "permissionPresets/catalog",
        request("r1", "permissionPresets/catalog", json!({"args":{}})),
    )
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(
        ok,
        json!({"type":"server-response","rpcId":"r1","result":{"ok":true,"value":{"options":[{"value":"read-only"}]}}})
    );

    let err: Value = call(
        "session/fail",
        request("r2", "session/fail", json!({"args":{}})),
    )
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(err["result"]["error"]["code"], "session/not-found");

    // Malformed envelopes answer with the invalid-request id, as the TypeScript Host does.
    let bad: Value = call("session/fail", "{}".into())
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(bad["rpcId"], "invalid-request");
    assert_eq!(bad["result"]["error"]["code"], "gateway/bad-request");
    let mismatch: Value = call("session/fail", request("r3", "other/method", json!({})))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mismatch["rpcId"], "invalid-request");

    // Unknown methods are plain 404s.
    assert_eq!(
        call("session/nope", request("r4", "session/nope", json!({})))
            .await
            .unwrap()
            .status(),
        404
    );
}

#[tokio::test]
async fn mux_events_ready_and_streams() {
    let (addr, _gw) = start().await;
    let cookie = sign_in(&addr).await;

    // The socket is refused without the cookie.
    assert!(
        tokio_tungstenite::connect_async(format!("ws://{addr}/api/remote.mux"))
            .await
            .is_err()
    );

    let mut req = format!("ws://{addr}/api/remote.mux")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert("cookie", cookie.parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();

    ws.send(Message::text(
        json!({"type":"open","streamId":"e1","endpoint":"$events","payload":{"args":{}}})
            .to_string(),
    ))
    .await
    .unwrap();
    let ready: Value =
        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(ready["streamId"], "e1");
    assert_eq!(ready["value"]["type"], "ready");
    assert_eq!(ready["value"]["host"]["home"], "/home/me");
    assert!(!ready["value"]["clientId"].as_str().unwrap().is_empty());

    ws.send(Message::text(
        json!({"type":"open","streamId":"s1","endpoint":"count","payload":{"args":{"n":2}}})
            .to_string(),
    ))
    .await
    .unwrap();
    ws.send(Message::text(
        json!({"type":"item","streamId":"s1","value":"ping"}).to_string(),
    ))
    .await
    .unwrap();
    let mut items = Vec::new();
    loop {
        let m: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        if m["type"] == "end" {
            break;
        }
        assert_eq!(m["streamId"], "s1");
        items.push(m["value"].clone());
    }
    assert_eq!(items, vec![json!(0), json!(1), json!({"echo":"ping"})]);

    // An unknown endpoint fails that stream, not the connection.
    ws.send(Message::text(
        json!({"type":"open","streamId":"s2","endpoint":"nope","payload":{}}).to_string(),
    ))
    .await
    .unwrap();
    let e: Value =
        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(
        (e["type"].as_str(), e["error"]["code"].as_str()),
        (Some("error"), Some("gateway/invocation-unavailable"))
    );
}

#[tokio::test]
async fn mux_events_publishes_live_fixture_and_ignores_uplink_backpressure() {
    let (addr, gw, server) = start_event_server().await;
    let mut ws = connect_mux(&addr).await;

    send_mux(
        &mut ws,
        json!({"type":"open","streamId":"e1","endpoint":"$events","payload":{"args":{}}}),
    )
    .await;
    let ready = next_stream_json(&mut ws, "e1").await;
    assert_eq!(ready["type"], "item");
    assert_eq!(ready["value"]["type"], "ready");
    assert_eq!(ready["value"]["host"]["home"], "/home/me");
    assert!(!ready["value"]["clientId"].as_str().unwrap().is_empty());

    // Shape copied from packages/api/session-controller/tests/client-apply.client.spec.ts.
    let added = session_added();
    gw.event_broadcaster().publish(added.clone());
    let item = next_stream_json(&mut ws, "e1").await;
    assert_eq!(item["type"], "item");
    assert_eq!(item["value"], added);

    let status = json!({"type":"emit","event":"api-session/status","args":["session-1",true]});
    let activity = json!({"type":"emit","event":"api-session/activity","args":["session-1",9]});
    gw.event_broadcaster().publish(status.clone());
    gw.event_broadcaster().publish(activity.clone());
    assert_eq!(next_stream_json(&mut ws, "e1").await["value"], status);
    assert_eq!(next_stream_json(&mut ws, "e1").await["value"], activity);

    // Gateway-owned streams release their uplink: neither a full buffer nor
    // half-close may fail or terminate the live event stream.
    for value in 0..80 {
        send_mux(
            &mut ws,
            json!({"type":"item","streamId":"e1","value":value}),
        )
        .await;
    }
    send_mux(&mut ws, json!({"type":"end","streamId":"e1"})).await;
    for value in 0..5 {
        send_mux(
            &mut ws,
            json!({"type":"item","streamId":"e1","value":value}),
        )
        .await;
    }
    let after_uplink =
        json!({"type":"emit","event":"api-session/status","args":["session-1",false]});
    gw.event_broadcaster().publish(after_uplink.clone());
    assert_eq!(next_stream_json(&mut ws, "e1").await["value"], after_uplink);

    shutdown_mux(&mut ws, server).await;
}

#[tokio::test]
async fn mux_events_cancel_is_local_and_stream_id_can_reopen() {
    let (addr, gw, server) = start_event_server().await;
    let mut ws = connect_mux(&addr).await;

    for stream_id in ["e1", "e2"] {
        send_mux(
            &mut ws,
            json!({"type":"open","streamId":stream_id,"endpoint":"$events","payload":{"args":{}}}),
        )
        .await;
        assert_eq!(
            next_stream_json(&mut ws, stream_id).await["value"]["type"],
            "ready"
        );
    }
    let added = session_added();
    gw.event_broadcaster().publish(added.clone());
    assert_eq!(
        next_stream_values(&mut ws, &["e1", "e2"]).await,
        vec![added.clone(), added]
    );

    send_mux(&mut ws, json!({"type":"cancel","streamId":"e1"})).await;
    wait_for_barrier(&mut ws, "b1").await;
    let status = json!({"type":"emit","event":"api-session/status","args":["session-1",true]});
    gw.event_broadcaster().publish(status.clone());
    assert_eq!(next_stream_json(&mut ws, "e2").await["value"], status);
    assert_no_stream_json(&mut ws, "e1").await;

    send_mux(
        &mut ws,
        json!({"type":"open","streamId":"e1","endpoint":"$events","payload":{"args":{}}}),
    )
    .await;
    assert_eq!(
        next_stream_json(&mut ws, "e1").await["value"]["type"],
        "ready"
    );
    let activity = json!({"type":"emit","event":"api-session/activity","args":["session-1",9]});
    gw.event_broadcaster().publish(activity.clone());
    assert_eq!(
        next_stream_values(&mut ws, &["e1", "e2"]).await,
        vec![activity.clone(), activity]
    );

    send_mux(&mut ws, json!({"type":"cancel","streamId":"e1"})).await;
    wait_for_barrier(&mut ws, "b2").await;
    gw.event_broadcaster().publish(session_added());
    assert_eq!(
        next_stream_json(&mut ws, "e2").await["value"],
        session_added()
    );
    assert_no_stream_json(&mut ws, "e1").await;

    shutdown_mux(&mut ws, server).await;
}

async fn start_event_server() -> (String, Gateway, JoinHandle<std::io::Result<()>>) {
    let gw = Gateway::new("launch-token", "/home/me", None);
    gw.stream("barrier", |_, _| {
        Box::pin(futures_util::stream::empty::<Result<Value, RpcError>>())
    });
    let (addr, serve) = gw.bind("127.0.0.1:0").await.unwrap();
    let server = tokio::spawn(serve);
    (format!("127.0.0.1:{}", addr.port()), gw, server)
}

async fn connect_mux(addr: &str) -> MuxSocket {
    let cookie = sign_in(addr).await;
    let mut req = format!("ws://{addr}/api/remote.mux")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert("cookie", cookie.parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

async fn send_mux(ws: &mut MuxSocket, message: Value) {
    ws.send(Message::text(message.to_string())).await.unwrap();
}

async fn recv_mux_json(ws: &mut MuxSocket) -> Value {
    loop {
        match ws.next().await.expect("mux socket stayed open").unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => panic!("mux socket closed before the expected frame"),
            Message::Binary(_) | Message::Frame(_) => {
                panic!("mux returned an unexpected binary frame")
            }
        }
    }
}

async fn next_stream_json(ws: &mut MuxSocket, stream_id: &str) -> Value {
    timeout(Duration::from_secs(5), async {
        loop {
            let frame = recv_mux_json(ws).await;
            if frame["streamId"] == stream_id {
                return frame;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for stream {stream_id}"))
}

async fn next_stream_values(ws: &mut MuxSocket, stream_ids: &[&str]) -> Vec<Value> {
    timeout(Duration::from_secs(5), async {
        let mut values: Vec<Option<Value>> = vec![None; stream_ids.len()];
        loop {
            let frame = recv_mux_json(ws).await;
            if let Some(index) = stream_ids.iter().position(|id| frame["streamId"] == *id) {
                values[index] = Some(frame["value"].clone());
            }
            if values.iter().all(Option::is_some) {
                return values.into_iter().map(Option::unwrap).collect::<Vec<_>>();
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for streams {stream_ids:?}"))
}

async fn assert_no_stream_json(ws: &mut MuxSocket, stream_id: &str) {
    let late = timeout(Duration::from_millis(250), async {
        loop {
            let frame = recv_mux_json(ws).await;
            if frame["streamId"] == stream_id {
                return frame;
            }
        }
    })
    .await;
    assert!(
        late.is_err(),
        "unexpected late frame for stream {stream_id}: {late:?}"
    );
}

async fn wait_for_barrier(ws: &mut MuxSocket, stream_id: &str) {
    send_mux(
        ws,
        json!({"type":"open","streamId":stream_id,"endpoint":"barrier","payload":{}}),
    )
    .await;
    assert_eq!(next_stream_json(ws, stream_id).await["type"], "end");
}

async fn shutdown_mux(ws: &mut MuxSocket, server: JoinHandle<std::io::Result<()>>) {
    let _ = ws.close(None).await;
    let _ = timeout(Duration::from_secs(5), async {
        while let Some(frame) = ws.next().await {
            if matches!(frame, Ok(Message::Close(_))) {
                break;
            }
        }
    })
    .await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

fn session_added() -> Value {
    json!({
        "type":"emit",
        "event":"api-session/added",
        "args":[{
            "agentAvailable":true,
            "sessionId":"session-1",
            "updatedAt":1,
            "running":false,
            "blank":true
        }]
    })
}

#[tokio::test]
async fn web_shell_serves_page_assets_and_plugin_combos() {
    let repo = std::env::temp_dir().join(format!("dsh-gw-repo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(repo.join("apps/web/dist/assets")).unwrap();
    std::fs::write(
        repo.join("apps/web/dist/index.html"),
        "<!doctype html><html><head></head><body><div id=\"root\"></div></body></html>",
    )
    .unwrap();
    std::fs::write(repo.join("apps/web/dist/assets/app.js"), "app()").unwrap();
    for (dir, name) in [
        ("a", "@deepseek-ai/dsh-api-gateway"),
        ("b", "@deepseek-ai/dsh-client-modules"),
    ] {
        std::fs::create_dir_all(repo.join(format!("packages/g/{dir}/lib"))).unwrap();
        std::fs::write(
            repo.join(format!("packages/g/{dir}/package.json")),
            format!("{{\"name\":\"{name}\"}}"),
        )
        .unwrap();
        std::fs::write(
            repo.join(format!("packages/g/{dir}/lib/client.js")),
            format!("load('{dir}');\n//# sourceMappingURL=client.js.map"),
        )
        .unwrap();
    }
    let gw = Gateway::new("t", "/h", None);
    gw.with_web_shell(&repo).unwrap();
    let (addr, serve) = gw.bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(serve);
    let addr = format!("127.0.0.1:{}", addr.port());
    let cookie = sign_in_with(&addr, "t").await;
    let client = reqwest::Client::new();
    let get = |path: &str| {
        client
            .get(format!("http://{addr}{path}"))
            .header("cookie", &cookie)
            .send()
    };

    let page = get("/").await.unwrap().text().await.unwrap();
    assert!(
        page.contains("__ModuleLoader__")
            && page.contains("__DSH_BOOT__")
            && page.contains("<div id=\"root\">")
    );
    assert_eq!(
        get("/assets/app.js").await.unwrap().text().await.unwrap(),
        "app()"
    );

    // The browser sends a query that itself starts with `?`.
    let body = get("/plugins/??@deepseek-ai/dsh-api-gateway/client.js,@deepseek-ai/dsh-client-modules/client.js&rev=abc").await.unwrap();
    assert_eq!(body.status(), 200);
    let text = body.text().await.unwrap();
    assert_eq!(
        text,
        "load('a');\n;\nload('b');\n;\n//# sourceMappingURL=??@deepseek-ai/dsh-api-gateway/client.js,@deepseek-ai/dsh-client-modules/client.js.map&rev=abc\n"
    );
    assert_eq!(
        get("/plugins/??@deepseek-ai/unknown/client.js")
            .await
            .unwrap()
            .status(),
        404
    );

    // Package images come from lib/assets and cannot escape it.
    std::fs::create_dir_all(repo.join("packages/g/a/lib/assets/icons")).unwrap();
    std::fs::write(
        repo.join("packages/g/a/lib/assets/icons/w.png"),
        [1u8, 2, 3],
    )
    .unwrap();
    let img = get("/plugins/@deepseek-ai/dsh-api-gateway/assets/icons/w.png")
        .await
        .unwrap();
    assert_eq!(img.status(), 200);
    assert_eq!(img.headers()["content-type"], "image/png");
    assert_eq!(img.bytes().await.unwrap().as_ref(), [1u8, 2, 3]);
    assert_eq!(
        get("/plugins/@deepseek-ai/dsh-api-gateway/assets/icons/none.png")
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        get("/plugins/@deepseek-ai/dsh-api-gateway/assets/..%2F..%2Fpackage.json")
            .await
            .unwrap()
            .status(),
        404
    );
    let _ = std::fs::remove_dir_all(repo);
}

async fn sign_in_with(addr: &str, token: &str) -> String {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let ok = client
        .get(format!("http://{addr}/?token={token}"))
        .send()
        .await
        .unwrap();
    ok.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}
