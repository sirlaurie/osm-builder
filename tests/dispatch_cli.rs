use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Command, Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const BATCH: &str = "10000000-0000-4000-8000-000000000001";
const TOKEN: &str = "dispatch-cli-test-token-000000000000";

struct Request {
    method: String,
    path: String,
    body: Value,
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    stopped: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn new(handler: impl Fn(&Request) -> Value + Send + 'static) -> Self {
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
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut parts = line.split_whitespace();
                let method = parts.next().unwrap().to_owned();
                let path = parts.next().unwrap().to_owned();
                let mut length = 0;
                let mut authorization = None;
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    let (key, value) = line.split_once(':').unwrap();
                    if key.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse::<usize>().unwrap();
                    } else if key.eq_ignore_ascii_case("authorization") {
                        authorization = Some(value.trim().to_owned());
                    }
                }
                assert_eq!(authorization, Some(format!("Bearer {TOKEN}")));
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).unwrap();
                let request = Request {
                    method,
                    path,
                    body: if bytes.is_empty() {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes).unwrap()
                    },
                };
                let response = serde_json::to_vec(&handler(&request)).unwrap();
                captured.lock().unwrap().push(request);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    response.len()
                )
                .unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        Self {
            url,
            requests,
            stopped,
            worker: Some(worker),
        }
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

fn project(url: &str) -> tempfile::TempDir {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&tests).unwrap();
    let project = tempfile::Builder::new()
        .prefix("dispatch-cli-")
        .tempdir_in(tests)
        .unwrap();
    fs::create_dir_all(project.path().join("config")).unwrap();
    fs::create_dir_all(project.path().join(".build")).unwrap();
    fs::write(
        project.path().join("config/runtime.json"),
        include_str!("../config/runtime.json"),
    )
    .unwrap();
    fs::write(
        project.path().join("config/regions.json"),
        r#"{"regions":[{"id":"test-region","extract":"europe/germany/berlin"}]}"#,
    )
    .unwrap();
    fs::write(
        project.path().join(".env"),
        format!(
            "R2_ACCOUNT_ID=00000000000000000000000000000000\nR2_BUCKET=test-bucket\nAWS_ACCESS_KEY_ID=test-access\nAWS_SECRET_ACCESS_KEY=test-secret\nOSM_WORKER_URL={url}\nOSM_PUBLISH_TOKEN={TOKEN}\n"
        ),
    )
    .unwrap();
    project
}

fn invoke(root: &Path, arguments: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_osm"))
        .current_dir(root.parent().unwrap())
        .env_clear()
        .env("OSM_COMPUTE_WORKERS", "1")
        .env("OSM_HTTP_ATTEMPTS", "1")
        .env("OSM_HTTP_TIMEOUT_MS", "2000")
        .env("OSM_HTTP_CONNECT_TIMEOUT_MS", "1000")
        .arg("--root")
        .arg(root)
        .args(arguments)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "osm {arguments:?} did not exit: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().unwrap()
}

#[test]
fn work_once_exits_for_terminal_batches_and_reports_failed_regions() {
    for (batch, failed) in [(None, 0), (Some(BATCH), 0), (Some(BATCH), 2)] {
        let server = Server::new(move |request| {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, "/admin/jobs/claim");
            json!({
                "batchId": batch,
                "lease": null,
                "pending": 0,
                "running": 0,
                "failed": failed,
                "retryAfterSeconds": 60
            })
        });
        let project = project(&server.url);
        let output = invoke(project.path(), &["work", "--once"]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.success(), failed == 0, "{stderr}");
        if failed > 0 {
            assert!(stderr.contains("Batch has 2 failed regions"), "{stderr}");
        }
        let requests = server.requests.lock().unwrap();
        assert!((1..=2).contains(&requests.len()));
        if failed == 0 {
            assert_eq!(requests.len(), 2);
        }
        let mut slots = std::collections::HashSet::new();
        for request in requests.iter().map(|request| &request.body) {
            assert_eq!(request["deviceId"].as_str().unwrap().len(), 36);
            assert_eq!(request["deviceId"], requests[0].body["deviceId"]);
            assert_eq!(request["requestId"].as_str().unwrap().len(), 36);
            assert_eq!(request["localRegions"], json!([]));
            let slot = request["slot"].as_u64().unwrap();
            assert!(slot <= 1 && slots.insert(slot));
        }
    }
}

#[test]
fn submit_only_and_jobs_bypass_the_lock_held_by_a_processing_device() {
    let server = Server::new(|request| match request.path.as_str() {
        "/admin/jobs/start" => {
            assert_eq!(request.method, "POST");
            json!({"batchId": BATCH})
        }
        "/admin/jobs" => {
            assert_eq!(request.method, "GET");
            json!({"batchId": BATCH, "pending": 1, "running": 0, "failed": 0})
        }
        path => panic!("Unexpected request: {} {path}", request.method),
    });
    let project = project(&server.url);
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(project.path().join(".build/pipeline.lock"))
        .unwrap();
    lock.lock().unwrap();

    let blocked = invoke(project.path(), &["work", "--once"]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("Another OSM pipeline is running"));
    assert!(server.requests.lock().unwrap().is_empty());

    for mode in ["bootstrap", "update"] {
        let output = invoke(project.path(), &[mode, "all", "--submit-only"]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains(BATCH));
    }
    let output = invoke(project.path(), &["jobs"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let jobs: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(jobs["batchId"], BATCH);
    assert_eq!(jobs["pending"], 1);

    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (request, mode) in requests[..2].iter().zip(["bootstrap", "update"]) {
        assert_eq!(request.path, "/admin/jobs/start");
        assert_eq!(request.body["mode"], mode);
        assert_eq!(
            request.body["regions"],
            json!([{"id": "test-region", "extract": "europe/germany/berlin"}])
        );
        assert_eq!(request.body["requestId"].as_str().unwrap().len(), 36);
    }
    assert_ne!(requests[0].body["requestId"], requests[1].body["requestId"]);
    assert_eq!(requests[2].path, "/admin/jobs");
}
