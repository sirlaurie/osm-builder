use aura_osm::dispatch::{Client, Guard, Lease, ReleaseOutcome, device_id};
use aura_osm::source::Region;
use chrono::{SecondsFormat, Utc};
use reqwest::header::HeaderValue;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

const BATCH: &str = "10000000-0000-4000-8000-000000000001";
const DEVICE: &str = "20000000-0000-4000-8000-000000000002";
const TOKEN: &str = "30000000-0000-4000-8000-000000000003";
const AUTHORIZATION: &str = "Bearer dispatch-test-secret";

fn lease() -> Lease {
    Lease {
        batch_id: BATCH.into(),
        region: "test-region".into(),
        extract: "europe/germany/berlin".into(),
        device_id: DEVICE.into(),
        slot: 0,
        token: TOKEN.into(),
        generation: 1,
        expires_at: (Utc::now() + chrono::Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        renew_after_seconds: 60,
    }
}

struct Reply {
    status: u16,
    body: Vec<u8>,
    disconnect: bool,
}

impl Reply {
    fn json(value: Value) -> Self {
        Self {
            status: 200,
            body: serde_json::to_vec(&value).unwrap(),
            disconnect: false,
        }
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    stopped: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn new(handler: impl Fn(&str, &Value) -> Reply + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = stopped.clone();
        let worker = thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 {
                    continue;
                }
                let path = line.split_whitespace().nth(1).unwrap().to_owned();
                let mut headers = HashMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let (key, value) = line.split_once(':').unwrap();
                    headers.insert(key.to_ascii_lowercase(), value.trim().to_owned());
                }
                assert_eq!(headers.get("authorization").unwrap(), AUTHORIZATION);
                let length = headers
                    .get("content-length")
                    .map(|value| value.parse::<usize>().unwrap())
                    .unwrap_or(0);
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).unwrap();
                let value = if bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes).unwrap()
                };
                captured.lock().unwrap().push((path.clone(), value.clone()));
                let reply = handler(&path, &value);
                if reply.disconnect {
                    continue;
                }
                let _ = write!(
                    stream,
                    "HTTP/1.1 {} Test\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                    reply.status,
                    reply.body.len()
                );
                let _ = stream.write_all(&reply.body);
            }
        });
        Self {
            url,
            requests,
            stopped,
            worker: Some(worker),
        }
    }

    fn client(&self, attempts: u32) -> Client {
        Client::new(
            reqwest::blocking::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            self.url.parse().unwrap(),
            HeaderValue::from_static(AUTHORIZATION),
            attempts,
            Duration::ZERO,
        )
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
    }
}

#[test]
fn request_identity_survives_lost_responses_but_changes_for_new_calls() {
    let calls = Mutex::new(HashMap::<String, usize>::new());
    let server = Server::new(move |path, value| {
        assert_eq!(value["requestId"].as_str().unwrap().len(), 36);
        let mut calls = calls.lock().unwrap();
        let count = calls.entry(path.to_owned()).or_default();
        *count += 1;
        let mut reply = match path {
            "/admin/jobs/start" => {
                assert_eq!(value["mode"], "bootstrap");
                Reply::json(json!({ "batchId": BATCH }))
            }
            "/admin/jobs/claim" => {
                assert_eq!(value["deviceId"], DEVICE);
                assert_eq!(value["slot"], 0);
                Reply::json(json!({ "batchId": null, "lease": null, "pending": 0,
                    "running": 0, "failed": 0, "retryAfterSeconds": 0 }))
            }
            _ => panic!("Unexpected dispatch path"),
        };
        reply.disconnect = *count == 1;
        reply
    });
    let client = server.client(2);
    let regions = [Region {
        id: "test-region".into(),
        extract: "europe/germany/berlin".into(),
    }];
    for _ in 0..2 {
        assert_eq!(client.start("bootstrap", &regions).unwrap(), BATCH);
        assert!(client.claim(DEVICE, &[], 0).unwrap().lease.is_none());
    }
    let requests = server.requests.lock().unwrap();
    for path in ["/admin/jobs/start", "/admin/jobs/claim"] {
        let ids: Vec<_> = requests
            .iter()
            .filter(|(route, _)| route == path)
            .map(|(_, value)| &value["requestId"])
            .collect();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], ids[1]);
        assert_ne!(ids[1], ids[2]);
    }
}

#[test]
fn claims_reject_unsafe_paths_invalid_fields_and_other_owners() {
    for (key, value) in [
        ("region", json!("../escape")),
        ("extract", json!("europe/../escape")),
        ("extract", json!("/europe/germany")),
        ("extract", json!("europe//germany")),
        ("deviceId", json!(BATCH)),
        ("slot", json!(1)),
        ("slot", json!(2)),
        ("slot", Value::Null),
        ("token", json!("secret-invalid-token")),
        ("batchId", json!(DEVICE)),
        ("generation", json!(0)),
        ("generation", json!(9_007_199_254_740_992u64)),
        ("renewAfterSeconds", json!(0)),
        ("expiresAt", json!("2000-01-01T00:00:00+08:00")),
        ("expiresAt", json!("2000-02-30T00:00:00Z")),
    ] {
        let mut invalid = serde_json::to_value(lease()).unwrap();
        invalid[key] = value;
        let server = Server::new(move |_, _| {
            Reply::json(json!({
                "batchId": BATCH, "lease": invalid, "pending": 0,
                "running": 1, "failed": 0, "retryAfterSeconds": 60,
            }))
        });
        assert!(
            server.client(2).claim(DEVICE, &[], 0).is_err(),
            "accepted {key}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[test]
fn server_clock_skew_does_not_expire_claimed_or_renewed_leases() {
    for timestamp in ["2000-01-01T00:00:00Z", "2100-01-01T00:00:00.000Z"] {
        let mut offered = lease();
        offered.expires_at = timestamp.into();
        let server = Server::new(move |path, _| match path {
            "/admin/jobs/claim" => Reply::json(json!({
                "batchId": BATCH, "lease": offered, "pending": 0,
                "running": 1, "failed": 0, "retryAfterSeconds": 60,
            })),
            "/admin/jobs/renew" => Reply::json(serde_json::to_value(&offered).unwrap()),
            _ => panic!("Unexpected dispatch path"),
        });
        let client = server.client(1);
        let claimed = client.claim(DEVICE, &[], 0).unwrap().lease.unwrap();
        let guard = Guard::start(client.clone(), claimed).unwrap();
        assert_eq!(guard.lease().unwrap().expires_at, timestamp);
        let renewed = client.renew(&guard.lease().unwrap()).unwrap();
        assert_eq!(renewed.expires_at, timestamp);
        drop(guard);
    }
}

#[test]
fn renewal_preserves_fencing_and_release_retries_the_same_lease() {
    let calls = Mutex::new(HashMap::<String, usize>::new());
    let server = Server::new(move |path, value| {
        assert_eq!(value["lease"]["token"], TOKEN);
        match path {
            "/admin/jobs/renew" => {
                let mut changed = value["lease"].clone();
                changed["generation"] = json!(2);
                Reply::json(changed)
            }
            "/admin/jobs/release" => {
                let outcome = value["outcome"].as_str().unwrap();
                assert!(["failed", "retry"].contains(&outcome));
                let mut calls = calls.lock().unwrap();
                let count = calls.entry(outcome.to_owned()).or_default();
                *count += 1;
                let mut reply = Reply::json(json!({ "success": true }));
                reply.disconnect = *count == 1;
                reply
            }
            _ => panic!("Unexpected dispatch path"),
        }
    });
    let client = server.client(2);
    assert!(client.renew(&lease()).is_err());
    let lease = lease();
    client.release(&lease, ReleaseOutcome::Failed).unwrap();
    client.release(&lease, ReleaseOutcome::Retry).unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests[1].1, requests[2].1);
    assert_eq!(requests[3].1, requests[4].1);
    assert_eq!(requests[1].1["outcome"], "failed");
    assert_eq!(requests[3].1["outcome"], "retry");
}

#[test]
fn claims_and_guards_keep_device_slots_independent() {
    let claims = Mutex::new(HashMap::<u8, usize>::new());
    let server = Server::new(move |path, value| match path {
        "/admin/jobs/claim" => {
            let slot = value["slot"].as_u64().unwrap() as u8;
            let mut offered = lease();
            offered.slot = slot;
            offered.renew_after_seconds = 1;
            if slot == 1 {
                offered.region = "second-region".into();
                offered.token = DEVICE.into();
            }
            let mut counts = claims.lock().unwrap();
            let count = counts.entry(slot).or_default();
            *count += 1;
            let mut reply = Reply::json(json!({
                "batchId": BATCH, "lease": offered, "pending": 0,
                "running": 2, "failed": 0, "retryAfterSeconds": 60,
            }));
            reply.disconnect = *count == 1;
            reply
        }
        "/admin/jobs/renew" if value["lease"]["slot"] == 0 => Reply {
            status: 409,
            ..Reply::json(json!({ "success": false, "error": "lease_lost" }))
        },
        "/admin/jobs/renew" => {
            let mut renewed = value["lease"].clone();
            renewed["renewAfterSeconds"] = json!(60);
            Reply::json(renewed)
        }
        _ => panic!("Unexpected dispatch path"),
    });
    let client = server.client(2);
    assert!(client.claim(DEVICE, &[], 2).is_err());
    assert!(server.requests.lock().unwrap().is_empty());
    let first = client.claim(DEVICE, &[], 0).unwrap().lease.unwrap();
    let second = client.claim(DEVICE, &[], 1).unwrap().lease.unwrap();
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].1, requests[1].1);
        assert_eq!(requests[2].1, requests[3].1);
        assert_ne!(requests[0].1["requestId"], requests[2].1["requestId"]);
    }
    let first = Guard::start(client.clone(), first).unwrap();
    let second = Guard::start(client, second).unwrap();
    let until = Instant::now() + Duration::from_secs(4);
    while (first.lease().is_ok() || second.lease().unwrap().renew_after_seconds != 60)
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(first.lease().is_err());
    assert_eq!(second.lease().unwrap().slot, 1);
    assert_eq!(second.lease().unwrap().renew_after_seconds, 60);
    drop(first);
    drop(second);
}

#[test]
fn renewal_cannot_move_a_lease_to_the_other_slot() {
    let server = Server::new(|path, value| {
        assert_eq!(path, "/admin/jobs/renew");
        let mut changed = value["lease"].clone();
        changed["slot"] = json!(1);
        Reply::json(changed)
    });
    assert!(server.client(2).renew(&lease()).is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[test]
fn errors_and_oversized_responses_are_bounded_and_sanitized() {
    for (status, body) in [
        (
            409,
            serde_json::to_vec(&json!({ "error": "lease_lost", "message": AUTHORIZATION }))
                .unwrap(),
        ),
        (400, format!("{AUTHORIZATION} {TOKEN}").into_bytes()),
        (200, format!("{AUTHORIZATION} {TOKEN}").into_bytes()),
        (200, vec![b' '; 1024 * 1024 + 1]),
    ] {
        let server = Server::new(move |_, _| Reply {
            status,
            body: body.clone(),
            disconnect: false,
        });
        let error = server.client(2).jobs().err().unwrap();
        let message = format!("{error:#}");
        assert!(!message.contains(AUTHORIZATION) && !message.contains(TOKEN));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[test]
fn device_identity_is_persistent_concurrent_and_never_replaces_invalid_data() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests/dispatch");
    fs::create_dir_all(&tests).unwrap();
    let data = tempfile::tempdir_in(tests).unwrap();
    let ids = thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| device_id(data.path()).unwrap()))
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(ids.iter().all(|id| id == &ids[0]));
    assert_eq!(device_id(data.path()).unwrap(), ids[0]);
    let path = data.path().join(".device-id");
    let invalid = format!("{}{}", ids[0], " ".repeat(100));
    fs::write(&path, &invalid).unwrap();
    assert!(device_id(data.path()).is_err());
    assert_eq!(fs::read_to_string(path).unwrap(), invalid);
}

#[test]
fn guard_rejects_lost_lease_and_drop_wakes_a_pending_renewal() {
    let server = Server::new(|path, _| {
        assert_eq!(path, "/admin/jobs/renew");
        Reply {
            status: 409,
            ..Reply::json(json!({ "success": false, "error": "lease_lost" }))
        }
    });
    let mut short = lease();
    short.expires_at = "2000-01-01T00:00:00Z".into();
    short.renew_after_seconds = 1;
    let guard = Guard::start(server.client(1), short).unwrap();
    assert!(guard.lease().is_ok());
    let until = Instant::now() + Duration::from_secs(4);
    while guard.lease().is_ok() && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(guard.lease().is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    drop(guard);
    let guard = Guard::start(server.client(1), lease()).unwrap();
    let started = Instant::now();
    drop(guard);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}
