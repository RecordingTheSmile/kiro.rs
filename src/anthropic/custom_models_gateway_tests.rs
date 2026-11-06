use super::*;
use crate::{admin::AdminService, model::custom_models};

#[test]
fn custom_models_bypass_upstream_list() {
    // 配置注册表是进程全局状态，隔离运行以免干扰其它模型列表测试。
    const CHILD_ENV: &str = "KIRO_RS_CUSTOM_MODELS_TEST_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "anthropic::gateway_tests::custom_models_tests::custom_models_bypass_upstream_list",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(check_custom_models());
}

async fn check_custom_models() {
    let path = std::env::temp_dir().join(format!("custom-models-{}.json", uuid::Uuid::new_v4()));
    std::fs::write(
        &path,
        serde_json::to_vec(&json!({"customModels": [{
            "id": "my-custom-model", "backendId": "Vendor.Future-v1",
            "contextWindow": 123456, "maxTokens": 7890
        }]}))
        .unwrap(),
    )
    .unwrap();
    let config = Config::load(&path).unwrap();
    custom_models::init(config.custom_models.clone());
    let fixture = fixture_with_config(Mode::Reply(&["hello"]), config).await;
    let manager = fixture
        .state
        .kiro_provider
        .as_ref()
        .unwrap()
        .token_manager();
    let service = AdminService::new(manager.clone(), vec!["fixture".into()]);
    let fetches = Arc::new(AtomicUsize::new(0));
    let fetch_count = fetches.clone();
    manager.set_test_models_fetcher(Arc::new(move |_| {
        fetch_count.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Ok(serde_json::from_value(json!({"models": [{"modelId": "listed-model"}]})).unwrap())
        })
    }));

    let client_keys = Arc::new(crate::admin::ClientKeyManager::new());
    client_keys.create_with_key("fixture".into(), None, None, "custom-model-test".into());
    let app = super::super::router::create_router_with_shared_provider(
        fixture.state.kiro_provider.clone(),
        false,
        ToolCompatibilityMode::Raw,
        Some(client_keys),
        None,
        None,
        None,
        None,
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    for backend in ["Vendor.Future-v1", "Vendor.Future-v2"] {
        if backend.ends_with("v2") {
            let before = fetches.load(Ordering::Relaxed);
            service
                .set_custom_models(
                    serde_json::from_value(json!({"models": [{
                        "id": "my-custom-model", "backendId": backend,
                        "contextWindow": 123456, "maxTokens": 7890
                    }]}))
                    .unwrap(),
                )
                .unwrap();
            assert_eq!(fetches.load(Ordering::Relaxed), before);
        }
        assert_eq!(
            Config::load(&path).unwrap().custom_models[0].backend_id,
            backend
        );
        manager.get_available_models_for(1).await.unwrap();
        let before = fetches.load(Ordering::Relaxed);
        assert!(manager.has_available_credentials(Some(backend), None));
        assert!(manager.has_available_credentials(Some(&backend.to_ascii_lowercase()), None));
        assert!(!manager.has_available_credentials(Some("unconfigured-model"), None));
        assert!(!manager.has_available_credentials(Some(backend), Some("other-group")));

        let response = client
            .get(format!("{base}/v1/models"))
            .header("x-api-key", "custom-model-test")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let models: serde_json::Value = response.json().await.unwrap();
        let custom = models["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["id"] == "my-custom-model")
            .unwrap();
        assert_eq!(custom["context_window"], 123456);
        assert_eq!(custom["max_tokens"], 7890);

        for mode in ["priority", "balanced"] {
            manager.set_load_balancing_mode(mode.into()).unwrap();
            for endpoint in [
                "/v1/messages",
                "/cc/v1/messages",
                "/v1/chat/completions",
                "/v1/responses",
            ] {
                for streaming in [false, true] {
                    let model = if streaming {
                        "MY-CUSTOM-MODEL-thinking"
                    } else {
                        "my-custom-model"
                    };
                    let response = client
                        .post(format!("{base}{endpoint}"))
                        .header("x-api-key", "custom-model-test")
                        .json(&json!({
                            "model": model, "max_tokens": 64, "stream": streaming,
                            "messages": [{"role": "user", "content": "hello"}],
                            "input": [{"role": "user", "content": "hello"}]
                        }))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(
                        response.status(),
                        StatusCode::OK,
                        "{mode} {endpoint} {streaming}"
                    );
                    let body = response.text().await.unwrap();
                    assert!(body.contains("hello"), "{body}");
                    assert_eq!(
                        fixture.bodies.lock().last().unwrap()["conversationState"]["currentMessage"]
                            ["userInputMessage"]["modelId"],
                        backend
                    );
                }
            }
        }
        assert_eq!(fetches.load(Ordering::Relaxed), before);
        manager.set_disabled(1, true).unwrap();
        assert!(!manager.has_available_credentials(Some(backend), None));
        manager.set_disabled(1, false).unwrap();
    }
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 32);
    service
        .set_custom_models(serde_json::from_value(json!({"models": []})).unwrap())
        .unwrap();
    manager.get_available_models_for(1).await.unwrap();
    assert!(!manager.has_available_credentials(Some("Vendor.Future-v2"), None));
    assert!(manager.has_available_credentials(Some("listed-model"), None));
    server.abort();
    std::fs::remove_file(path).unwrap();
}
