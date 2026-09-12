use aura_osm::config::{Environment, Runtime};
use aura_osm::format::{MAX_BLOCK, MAX_CURRENT, hash_bytes};
use aura_osm::publish::{Progress, PublishConfig, Publisher};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const TOKEN: &str = "test-publish-secret-test-publish-secret";
const SECRET: &str = "test-aws-secret";
const REVISION: &str = "12345678-1234-1234-1234-123456789abc";
const NEW_REVISION: &str = "87654321-4321-4321-4321-cba987654321";

struct Wave {
    total: usize,
    count: Mutex<usize>,
    ready: Condvar,
}

impl Wave {
    fn new(total: usize) -> Self {
        Self {
            total,
            count: Mutex::new(0),
            ready: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut count = self.count.lock().unwrap();
        *count += 1;
        if *count == self.total {
            self.ready.notify_all();
        }
        let (_count, timeout) = self
            .ready
            .wait_timeout_while(count, Duration::from_secs(3), |count| *count < self.total)
            .unwrap();
        assert!(!timeout.timed_out(), "first upload wave timed out");
    }
}

fn temporary() -> TempDir {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests/rust-publish");
    fs::create_dir_all(&parent).unwrap();
    tempfile::tempdir_in(parent).unwrap()
}

fn values(url: &str, overrides: &[(&str, &str)]) -> Environment {
    let mut values: BTreeMap<String, String> = [
        ("R2_ACCOUNT_ID", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ("R2_BUCKET", "osm-test"),
        ("AWS_ACCESS_KEY_ID", "test-access-key"),
        ("AWS_SECRET_ACCESS_KEY", SECRET),
        ("OSM_WORKER_URL", url),
        ("OSM_PUBLISH_TOKEN", TOKEN),
        ("OSM_HTTP_RETRY_DELAY_MS", "0"),
        ("OSM_HTTP_TIMEOUT_MS", "2000"),
        ("OSM_HTTP_CONNECT_TIMEOUT_MS", "1000"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect();
    values.extend(
        overrides
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
    );
    Environment::from_values(values)
}

fn publisher(server: &Server, overrides: &[(&str, &str)]) -> Publisher {
    let environment = values(&server.url, overrides);
    let runtime = Runtime::from_json(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        include_str!("../config/runtime.json"),
        &environment,
    )
    .unwrap();
    Publisher::new(
        &runtime,
        &PublishConfig::from_environment(&environment).unwrap(),
    )
    .unwrap()
    .with_local_r2_endpoint(&server.url)
    .unwrap()
}

struct Build {
    directory: TempDir,
    release: Value,
    manifest: Value,
    keys: Vec<String>,
    manifest_key: String,
}

impl Build {
    fn new(count: usize) -> Self {
        let directory = temporary();
        fs::create_dir(directory.path().join("blocks")).unwrap();
        fs::create_dir(directory.path().join("manifests")).unwrap();
        let mut ids: Vec<_> = (1..=count).map(|id| format!("osm_node_{id}")).collect();
        ids.sort();
        let mut hashes = Vec::new();
        let mut keys = Vec::new();
        for id in ids {
            let bytes = serde_json::to_vec(&json!([{ "id": id, "lat": 0, "lon": 0, "tags": { "name": "Cafe", "amenity": "cafe" } }])).unwrap();
            let hash = hash_bytes(&bytes);
            let key = format!("blocks/{hash}.json");
            fs::write(directory.path().join(&key), bytes).unwrap();
            hashes.push(hash);
            keys.push(key);
        }
        let cells = if count == 0 {
            json!({})
        } else {
            json!({ "9000_18000": hashes })
        };
        let manifest = json!({
            "schema": 1, "region": "test-region", "sourceTimestamp": "2026-09-10T00:00:00Z",
            "sourceSequence": 42, "sourceSHA256": "1".repeat(64),
            "coverage": { "type": "Polygon", "coordinates": [[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]] },
            "cells": cells, "count": count,
        });
        let mut build = Self {
            directory,
            release: Value::Null,
            manifest,
            keys,
            manifest_key: String::new(),
        };
        build.save_manifest();
        build
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }

    fn save_manifest(&mut self) {
        let bytes = serde_json::to_vec(&self.manifest).unwrap();
        let hash = hash_bytes(&bytes);
        self.manifest_key = format!("manifests/{hash}.json");
        fs::write(self.path().join(&self.manifest_key), bytes).unwrap();
        self.release = json!({ "region": self.manifest["region"], "manifest": hash,
            "sourceTimestamp": self.manifest["sourceTimestamp"], "count": self.manifest["count"] });
        self.save_release();
    }

    fn save_release(&self) {
        fs::write(
            self.path().join("release.json"),
            serde_json::to_vec(&self.release).unwrap(),
        )
        .unwrap();
    }

    fn replace_block(&mut self, pois: Value) {
        let bytes = serde_json::to_vec(&pois).unwrap();
        let hash = hash_bytes(&bytes);
        let key = format!("blocks/{hash}.json");
        fs::write(self.path().join(&key), bytes).unwrap();
        self.keys = vec![key];
        self.manifest["cells"] = json!({ "9000_18000": [hash] });
        self.manifest["count"] = json!(pois.as_array().unwrap().len());
        self.save_manifest();
    }
}

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl Request {
    fn read(stream: &TcpStream) -> Option<Self> {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        let mut pieces = line.split_whitespace();
        let method = pieces.next()?.to_owned();
        let path = pieces.next()?.to_owned();
        let mut headers = BTreeMap::new();
        loop {
            line.clear();
            reader.read_line(&mut line).ok()?;
            if line == "\r\n" {
                break;
            }
            let (name, value) = line.split_once(':')?;
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        let length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        assert!(length <= 8 * 1024 * 1024);
        let mut body = vec![0; length];
        reader.read_exact(&mut body).ok()?;
        Some(Self {
            method,
            path,
            headers,
            body,
        })
    }
}

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    disconnect: bool,
    delay: Duration,
}

impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            disconnect: false,
            delay: Duration::ZERO,
        }
    }
    fn json(value: &Value) -> Self {
        Self {
            body: serde_json::to_vec(value).unwrap(),
            ..Self::status(200)
        }
    }
    fn write(&self, mut stream: TcpStream) {
        if self.disconnect {
            return;
        }
        thread::sleep(self.delay);
        let mut header = format!("HTTP/1.1 {} Test\r\nConnection: close\r\n", self.status);
        if !self
            .headers
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case("content-length"))
        {
            header.push_str(&format!("Content-Length: {}\r\n", self.body.len()));
        }
        for (name, value) in &self.headers {
            header.push_str(&format!("{name}: {value}\r\n"));
        }
        header.push_str("\r\n");
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&self.body);
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn new(handler: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let handler = Arc::new(handler);
        let worker = thread::spawn(move || {
            let mut handlers = Vec::new();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                if worker_stop.load(Ordering::SeqCst) {
                    break;
                }
                let handler = handler.clone();
                let captured = captured.clone();
                handlers.push(thread::spawn(move || {
                    if let Some(request) = Request::read(&stream) {
                        captured.lock().unwrap().push(request.clone());
                        handler(&request).write(stream);
                    }
                }));
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });
        Self {
            url: format!("http://{address}"),
            requests,
            stop,
            worker: Some(worker),
        }
    }

    fn events(&self) -> Vec<(String, String)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| (request.method.clone(), request.path.clone()))
            .collect()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
    }
}

struct Remote {
    release: Value,
    current: Value,
    objects: HashMap<String, (usize, Option<String>)>,
    state_replies: VecDeque<Reply>,
    put_status: Option<u16>,
    race: Option<u16>,
    missing_race: bool,
    lost_put: bool,
    lost_publish: bool,
    publish_status: Option<u16>,
    changed_revision: bool,
    acknowledge_without_commit: bool,
    bad_verification: bool,
    publish_bases: Vec<Value>,
}

impl Remote {
    fn new(build: &Build) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            release: build.release.clone(),
            current: Value::Null,
            objects: HashMap::new(),
            state_replies: VecDeque::new(),
            put_status: None,
            race: None,
            missing_race: false,
            lost_put: false,
            lost_publish: false,
            publish_status: None,
            changed_revision: false,
            acknowledge_without_commit: false,
            bad_verification: false,
            publish_bases: Vec::new(),
        }))
    }

    fn server(remote: &Arc<Mutex<Self>>) -> Server {
        let remote = remote.clone();
        Server::new(move |request| remote.lock().unwrap().handle(request))
    }

    fn handle(&mut self, request: &Request) -> Reply {
        if request.path.starts_with("/admin/") {
            assert_eq!(
                request.headers.get("authorization").unwrap(),
                &format!("Bearer {TOKEN}")
            );
            if request.path == "/admin/state" {
                assert_eq!(request.method, "GET");
                assert!(request.body.is_empty());
                return self
                    .state_replies
                    .pop_front()
                    .unwrap_or_else(|| Reply::json(&self.current));
            }
            assert_eq!(request.path, "/admin/publish");
            assert_eq!(request.method, "POST");
            let value: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(value["region"], self.release["region"]);
            assert_eq!(value["manifest"], self.release["manifest"]);
            self.publish_bases.push(value["baseRevision"].clone());
            if self.changed_revision {
                self.current = json!({ "schema": 1, "revision": NEW_REVISION, "regions": [] });
            }
            if let Some(status) = self.publish_status {
                return Reply::status(status);
            }
            if !self.acknowledge_without_commit {
                self.current = json!({ "schema": 1, "revision": REVISION, "regions": [{
                    "region": self.release["region"], "manifest": self.release["manifest"],
                    "sourceTimestamp": self.release["sourceTimestamp"], "bbox": [-1,-1,1,1],
                }] });
            }
            return Reply {
                disconnect: self.lost_publish,
                ..Reply::json(&json!({ "success": true }))
            };
        }
        let key = request.path.strip_prefix("/osm-test/").unwrap();
        let authorization = request.headers.get("authorization").unwrap();
        assert!(authorization.starts_with("AWS4-HMAC-SHA256 Credential=test-access-key/"));
        assert!(authorization.contains("/auto/s3/aws4_request"));
        assert!(request.headers.contains_key("x-amz-date"));
        assert_eq!(
            request.headers.get("x-amz-content-sha256").unwrap(),
            &hash_bytes(&request.body)
        );
        if request.method == "HEAD" {
            return match self.objects.get(key) {
                None => Reply::status(404),
                Some((size, hash)) => {
                    let mut reply = Reply::status(200);
                    reply.headers.push((
                        "Content-Length".into(),
                        (size + usize::from(self.bad_verification)).to_string(),
                    ));
                    if let Some(hash) = hash {
                        reply
                            .headers
                            .push(("x-amz-meta-sha256".into(), hash.clone()));
                    }
                    reply
                }
            };
        }
        assert_eq!(request.method, "PUT");
        assert_eq!(request.headers.get("if-none-match").unwrap(), "*");
        assert!(authorization.contains("if-none-match"));
        assert!(authorization.contains("x-amz-meta-sha256"));
        assert_eq!(
            request.headers.get("x-amz-meta-sha256").unwrap(),
            &hash_bytes(&request.body)
        );
        assert_eq!(
            request.headers.get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(
            request.headers.get("cache-control").unwrap(),
            "public, max-age=31536000, immutable"
        );
        if let Some(status) = self.put_status {
            return Reply::status(status);
        }
        if self.objects.contains_key(key) {
            return Reply::status(412);
        }
        if !self.missing_race {
            self.objects.insert(
                key.to_owned(),
                (request.body.len(), Some(hash_bytes(&request.body))),
            );
        }
        if self.lost_put {
            self.lost_put = false;
            return Reply {
                disconnect: true,
                ..Reply::status(200)
            };
        }
        Reply::status(self.race.unwrap_or(200))
    }
}

#[test]
fn validates_uploads_blocks_then_manifest_publishes_and_resumes() {
    let build = Build::new(1);
    let remote = Remote::new(&build);
    let server = Remote::server(&remote);
    let publisher = publisher(&server, &[]);
    let mut progress = Vec::new();
    let receipt = publisher
        .publish(build.path(), |event| progress.push(event))
        .unwrap();
    assert_eq!(receipt["uploaded"], 2);
    assert_eq!(receipt["reused"], 0);
    let saved: Value =
        serde_json::from_slice(&fs::read(build.path().join("publish-receipt.json")).unwrap())
            .unwrap();
    assert_eq!(saved, receipt);
    assert!(!saved.to_string().contains(TOKEN));
    assert!(!saved.to_string().contains(SECRET));
    assert_eq!(
        server
            .events()
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        [
            "GET", "HEAD", "PUT", "HEAD", "HEAD", "PUT", "HEAD", "POST", "GET"
        ]
    );
    let puts: Vec<_> = server
        .events()
        .into_iter()
        .filter(|(method, _)| method == "PUT")
        .map(|(_, path)| path)
        .collect();
    assert_eq!(
        puts,
        [
            format!("/osm-test/{}", build.keys[0]),
            format!("/osm-test/{}", build.manifest_key)
        ]
    );
    assert_eq!(
        progress
            .iter()
            .filter(|event| event.stage == "upload")
            .map(|event| event.completed)
            .collect::<Vec<_>>(),
        [Some(0), Some(1), Some(2)]
    );
    assert_eq!(progress.last().unwrap().stage, "done");
    let second = publisher.publish(build.path(), |_| {}).unwrap();
    assert_eq!(second["uploaded"], 0);
    assert_eq!(second["reused"], 2);
    assert_eq!(second["bytes"], 0);
}

#[test]
fn state_only_reads_validate_without_accessing_r2() {
    let build = Build::new(0);
    let remote = Remote::new(&build);
    let server = Remote::server(&remote);
    let publisher = publisher(&server, &[]);
    assert!(publisher.read_state().unwrap().is_none());
    remote.lock().unwrap().current = json!({ "schema": 1, "revision": REVISION, "regions": [] });
    assert_eq!(publisher.read_state().unwrap().unwrap().revision, REVISION);
    assert_eq!(
        server.events(),
        [
            ("GET".into(), "/admin/state".into()),
            ("GET".into(), "/admin/state".into())
        ]
    );
}

#[test]
fn all_local_failures_precede_network() {
    for case in 0..22 {
        let mut build = Build::new(1);
        match case {
            0 => build.manifest["sourceTimestamp"] = json!("2026-02-30T00:00:00Z"),
            1 => build.manifest["cells"]["9000_18000"]
                .as_array_mut()
                .unwrap()
                .push(json!("f".repeat(64))),
            2 => build.manifest["count"] = json!(2),
            3 => build.manifest["coverage"]["coordinates"][0]
                .as_array_mut()
                .unwrap()
                .pop()
                .map(|_| ())
                .unwrap(),
            4 => {
                build.manifest["coverage"]["coordinates"][0] =
                    json!([[0, 0], [1, 1], [2, 2], [0, 0]])
            }
            5 => {
                build.manifest["cells"] = json!({ "18000_36000": [build.keys[0].trim_start_matches("blocks/").trim_end_matches(".json")] })
            }
            6 => build.manifest["cells"]["9000_18000"]
                .as_array_mut()
                .unwrap()
                .push(json!(
                    build.keys[0]
                        .trim_start_matches("blocks/")
                        .trim_end_matches(".json")
                )),
            7 => {
                build
                    .manifest
                    .as_object_mut()
                    .unwrap()
                    .remove("sourceSequence");
            }
            8 => build.manifest["coverage"]["coordinates"][0] = json!(vec![[0, 0]; 100001]),
            _ => {}
        }
        build.save_manifest();
        match case {
            9 => {
                build.release["manifest"] = json!("../escape");
                build.save_release();
            }
            10 => {
                build.release["region"] = Value::Null;
                build.save_release();
            }
            11 => fs::write(build.path().join(&build.keys[0]), b"[]").unwrap(),
            12 => fs::write(build.path().join(&build.keys[0]), vec![b' '; MAX_BLOCK + 1]).unwrap(),
            13..=21 => {
                let mut poi =
                    json!({ "id":"osm_node_1", "lat":0, "lon":0, "tags":{"name":"Cafe"} });
                match case {
                    13 => poi["tags"]["name"] = json!(["Cafe"]),
                    14 => poi["lat"] = json!(91),
                    15 => poi["lat"] = json!(1),
                    16 => poi["id"] = json!("osm_node_9007199254740993"),
                    17 => poi["tags"]["name"] = json!(" "),
                    20 => poi["lon"] = json!(180),
                    21 => poi["id"] = json!("osm_node_01"),
                    _ => {}
                }
                let pois = if case == 18 {
                    json!([poi.clone(), poi])
                } else if case == 19 {
                    let mut second = poi.clone();
                    second["id"] = json!("osm_node_2");
                    json!([second, poi])
                } else {
                    json!([poi])
                };
                build.replace_block(pois);
            }
            _ => {}
        }
        let remote = Remote::new(&build);
        let server = Remote::server(&remote);
        let result = publisher(&server, &[]).publish(build.path(), |_| {});
        assert!(result.is_err(), "case {case} accepted");
        assert!(server.events().is_empty(), "case {case} contacted network");
    }
}

#[test]
fn local_symlinks_and_changed_files_are_rejected() {
    use std::os::unix::fs::symlink;
    let build = Build::new(1);
    let outside = temporary();
    let block = build.path().join(&build.keys[0]);
    fs::rename(&block, outside.path().join("block.json")).unwrap();
    symlink(outside.path().join("block.json"), &block).unwrap();
    let remote = Remote::new(&build);
    let server = Remote::server(&remote);
    assert!(
        publisher(&server, &[])
            .publish(build.path(), |_| {})
            .is_err()
    );
    assert!(server.events().is_empty());

    let build = Build::new(1);
    let remote = Remote::new(&build);
    let block = build.path().join(&build.keys[0]);
    let changed = Arc::new(AtomicBool::new(false));
    let changed_request = changed.clone();
    let server = Server::new(move |request| {
        if request.path == "/admin/state" && !changed_request.swap(true, Ordering::SeqCst) {
            fs::write(&block, b"[]").unwrap();
        }
        remote.lock().unwrap().handle(request)
    });
    assert!(
        publisher(&server, &[])
            .publish(build.path(), |_| {})
            .is_err()
    );
    assert!(
        !server
            .events()
            .iter()
            .any(|(method, _)| method == "PUT" || method == "POST")
    );
}

#[test]
fn immutable_mismatches_fail_without_overwrite() {
    for (size_delta, metadata) in [(0, None), (0, Some("f".repeat(64))), (1, None)] {
        let build = Build::new(1);
        let remote = Remote::new(&build);
        let size = fs::metadata(build.path().join(&build.keys[0]))
            .unwrap()
            .len() as usize;
        let metadata = if size_delta == 1 {
            Some(
                build.keys[0]
                    .trim_start_matches("blocks/")
                    .trim_end_matches(".json")
                    .to_owned(),
            )
        } else {
            metadata
        };
        remote
            .lock()
            .unwrap()
            .objects
            .insert(build.keys[0].clone(), (size + size_delta, metadata));
        let server = Remote::server(&remote);
        let error = publisher(&server, &[])
            .publish(build.path(), |_| {})
            .unwrap_err();
        assert!(error.to_string().contains("Immutable object mismatch"));
        assert!(
            !server
                .events()
                .iter()
                .any(|(method, _)| method == "PUT" || method == "POST")
        );
    }
}

#[test]
fn conditional_races_and_lost_upload_acknowledgements_require_verified_objects() {
    for race in [409, 412, 0] {
        let build = Build::new(1);
        let remote = Remote::new(&build);
        {
            let mut remote = remote.lock().unwrap();
            remote.race = (race != 0).then_some(race);
            remote.lost_put = race == 0;
        }
        let server = Remote::server(&remote);
        let receipt = publisher(&server, &[])
            .publish(build.path(), |_| {})
            .unwrap();
        assert_eq!(receipt["reused"], if race == 0 { 1 } else { 2 });
    }
    let build = Build::new(1);
    let remote = Remote::new(&build);
    {
        let mut remote = remote.lock().unwrap();
        remote.race = Some(412);
        remote.missing_race = true;
    }
    let server = Remote::server(&remote);
    assert!(
        publisher(&server, &[])
            .publish(build.path(), |_| {})
            .unwrap_err()
            .to_string()
            .contains("Concurrent upload did not create object")
    );
    assert!(!server.events().iter().any(|(method, _)| method == "POST"));
}

#[test]
fn failed_upload_or_verification_blocks_manifest_and_completion() {
    for verify in [false, true] {
        let build = Build::new(1);
        let remote = Remote::new(&build);
        {
            let mut remote = remote.lock().unwrap();
            if verify {
                remote.bad_verification = true;
            } else {
                remote.put_status = Some(503);
            }
        }
        let server = Remote::server(&remote);
        let mut progress = Vec::new();
        assert!(
            publisher(&server, &[])
                .publish(build.path(), |event| progress.push(event))
                .is_err()
        );
        assert!(
            !server
                .events()
                .iter()
                .any(|(method, path)| method == "POST" || path.contains("manifests/"))
        );
        assert!(
            !progress
                .iter()
                .any(|event| matches!(event.stage, "done" | "receipt" | "publish"))
        );
        assert!(!build.path().join("publish-receipt.json").exists());
        if !verify {
            assert_eq!(
                server
                    .events()
                    .iter()
                    .filter(|(method, _)| method == "PUT")
                    .count(),
                3
            );
        }
    }
}

#[test]
fn publication_ack_recovery_and_cas_do_not_rebase() {
    let build = Build::new(1);
    let remote = Remote::new(&build);
    remote.lock().unwrap().lost_publish = true;
    let server = Remote::server(&remote);
    let receipt = publisher(&server, &[])
        .publish(build.path(), |_| {})
        .unwrap();
    assert_eq!(receipt["revision"], REVISION);
    assert_eq!(remote.lock().unwrap().publish_bases, [Value::Null]);

    for (status, changed, expected) in [
        (409, false, "Publish conflict"),
        (503, true, "remote revision changed"),
        (503, false, "after 2 attempts"),
    ] {
        let build = Build::new(1);
        let remote = Remote::new(&build);
        {
            let mut remote = remote.lock().unwrap();
            remote.publish_status = Some(status);
            remote.changed_revision = changed;
            remote.current = json!({ "schema":1, "revision":REVISION, "regions":[] });
        }
        let server = Remote::server(&remote);
        let error = publisher(&server, &[("OSM_HTTP_ATTEMPTS", "2")])
            .publish(build.path(), |_| {})
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert!(
            remote
                .lock()
                .unwrap()
                .publish_bases
                .iter()
                .all(|base| base == REVISION)
        );
        assert_eq!(
            remote.lock().unwrap().publish_bases.len(),
            if status == 503 && !changed { 2 } else { 1 }
        );
        assert!(!build.path().join("publish-receipt.json").exists());
    }
}

#[test]
fn acknowledged_publish_must_be_current_and_receipt_symlink_is_not_followed() {
    let build = Build::new(1);
    let remote = Remote::new(&build);
    remote.lock().unwrap().acknowledge_without_commit = true;
    let server = Remote::server(&remote);
    assert!(
        publisher(&server, &[])
            .publish(build.path(), |_| {})
            .unwrap_err()
            .to_string()
            .contains("acknowledged but target manifest")
    );
    assert!(!build.path().join("publish-receipt.json").exists());

    let build = Build::new(1);
    let outside = temporary();
    let marker = outside.path().join("marker");
    fs::write(&marker, b"keep").unwrap();
    std::os::unix::fs::symlink(&marker, build.path().join("publish-receipt.json")).unwrap();
    let remote = Remote::new(&build);
    let server = Remote::server(&remote);
    assert!(
        publisher(&server, &[])
            .publish(build.path(), |_| {})
            .is_err()
    );
    assert_eq!(fs::read(marker).unwrap(), b"keep");
}

#[test]
fn state_errors_are_bounded_sanitized_and_redirects_never_followed() {
    for (status, attempts, message) in [
        (503, 3, "State request failed"),
        (401, 1, "State request rejected"),
        (302, 1, "State request rejected"),
    ] {
        let destination = Server::new(|_| panic!("redirect leaked credentials"));
        let url = destination.url.clone();
        let server = Server::new(move |_| Reply {
            headers: vec![("Location".into(), url.clone())],
            body: format!("{TOKEN} {SECRET}").into_bytes(),
            ..Reply::status(status)
        });
        let error = publisher(&server, &[])
            .read_state()
            .unwrap_err()
            .to_string();
        assert!(error.contains(message));
        assert!(!error.contains(TOKEN) && !error.contains(SECRET));
        assert_eq!(server.events().len(), attempts);
        assert!(destination.events().is_empty());
    }
    for (body, message) in [
        (
            format!("{TOKEN} {SECRET}").into_bytes(),
            "Invalid state response JSON",
        ),
        (vec![b' '; MAX_CURRENT + 1], "State response is too large"),
        (
            serde_json::to_vec(&json!({ "schema":1, "revision":"invalid", "regions":[] })).unwrap(),
            "Invalid state response schema",
        ),
        (
            serde_json::to_vec(&json!({ "schema":TOKEN, "revision":REVISION, "regions":[] }))
                .unwrap(),
            "Invalid state response schema",
        ),
    ] {
        let server = Server::new(move |_| Reply {
            body: body.clone(),
            ..Reply::status(200)
        });
        let error = format!("{:#}", publisher(&server, &[]).read_state().unwrap_err());
        assert!(error.contains(message), "{error}");
        assert!(!error.contains(TOKEN) && !error.contains(SECRET));
        assert_eq!(server.events().len(), 1);
    }
}

#[test]
fn state_body_truncation_and_timeout_retry_but_invalid_schema_does_not() {
    let build = Build::new(0);
    let remote = Remote::new(&build);
    remote.lock().unwrap().state_replies.push_back(Reply {
        headers: vec![("Content-Length".into(), "40".into())],
        body: b"nu".to_vec(),
        ..Reply::status(200)
    });
    let server = Remote::server(&remote);
    assert!(publisher(&server, &[]).read_state().unwrap().is_none());
    assert_eq!(server.events().len(), 2);

    let server = Server::new(|_| Reply {
        body: b"null".to_vec(),
        delay: Duration::from_millis(150),
        ..Reply::status(200)
    });
    let started = Instant::now();
    assert!(
        publisher(
            &server,
            &[("OSM_HTTP_ATTEMPTS", "2"), ("OSM_HTTP_TIMEOUT_MS", "30")]
        )
        .read_state()
        .is_err()
    );
    assert_eq!(server.events().len(), 2);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn unsafe_configuration_and_nonlocal_test_endpoints_are_rejected() {
    for url in [
        "http://osm.example.com",
        "https://user:pass@osm.example.com",
        "https://osm.example.com/path",
        "https://osm.example.com?token=abc",
    ] {
        assert!(PublishConfig::from_environment(&values(url, &[])).is_err());
    }
    for (key, value) in [
        ("AWS_SECRET_ACCESS_KEY", ""),
        ("OSM_PUBLISH_TOKEN", "short"),
        ("R2_ACCOUNT_ID", "bad"),
        ("R2_BUCKET", "Upper"),
    ] {
        assert!(
            PublishConfig::from_environment(&values("https://osm.example.com", &[(key, value)]))
                .is_err()
        );
    }
    let server = Server::new(|_| panic!("must not request"));
    assert!(
        publisher(&server, &[])
            .with_local_r2_endpoint("https://remote.example.com")
            .is_err()
    );
    assert!(server.events().is_empty());
    for concurrency in ["0", "65", "-1", "1.5", "16jobs", "", " 16", "016"] {
        let environment = values(&server.url, &[("OSM_UPLOAD_CONCURRENCY", concurrency)]);
        assert!(
            Runtime::from_json(
                Path::new(env!("CARGO_MANIFEST_DIR")),
                include_str!("../config/runtime.json"),
                &environment
            )
            .is_err()
        );
    }
}

#[test]
fn uploads_respect_bound_and_manifest_waits_for_all_verifications() {
    for concurrency in [1, 3, 16] {
        let build = Build::new(concurrency + 2);
        let remote = Remote::new(&build);
        let active = Arc::new(Mutex::new(HashSet::new()));
        let started = Arc::new(Mutex::new(HashSet::new()));
        let verified = Arc::new(Mutex::new(HashSet::new()));
        let peak = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Wave::new(concurrency));
        let expected = build.keys.len();
        let active_handler = active.clone();
        let started_handler = started.clone();
        let verified_handler = verified.clone();
        let peak_handler = peak.clone();
        let server = Server::new(move |request| {
            if request.path.contains("/blocks/") {
                let first = started_handler.lock().unwrap().insert(request.path.clone());
                if first {
                    let length = {
                        let mut active = active_handler.lock().unwrap();
                        active.insert(request.path.clone());
                        active.len()
                    };
                    assert!(length <= concurrency);
                    {
                        let mut peak = peak_handler.lock().unwrap();
                        *peak = (*peak).max(length);
                    }
                    if started_handler.lock().unwrap().len() <= concurrency {
                        barrier.wait();
                    }
                }
            } else if request.path.contains("/manifests/") {
                assert!(active_handler.lock().unwrap().is_empty());
                assert_eq!(verified_handler.lock().unwrap().len(), expected);
            }
            let reply = remote.lock().unwrap().handle(request);
            if request.method == "HEAD" && request.path.contains("/blocks/") && reply.status == 200
            {
                active_handler.lock().unwrap().remove(&request.path);
                verified_handler
                    .lock()
                    .unwrap()
                    .insert(request.path.clone());
            }
            reply
        });
        let mut progress: Vec<Progress> = Vec::new();
        let configured = concurrency.to_string();
        publisher(&server, &[("OSM_UPLOAD_CONCURRENCY", &configured)])
            .publish(build.path(), |event| progress.push(event))
            .unwrap();
        assert_eq!(*peak.lock().unwrap(), concurrency);
        assert_eq!(
            progress
                .iter()
                .filter(|event| event.stage == "upload")
                .map(|event| event.completed.unwrap())
                .collect::<Vec<_>>(),
            (0..=(expected + 1)).collect::<Vec<_>>()
        );
    }
}

#[test]
fn failed_object_stops_dispatch_and_drains_inflight_objects() {
    let build = Build::new(6);
    let remote = Remote::new(&build);
    let failing = format!("/osm-test/{}", build.keys[0]);
    let started = Arc::new(Mutex::new(HashSet::new()));
    let started_handler = started.clone();
    let first_wave = Arc::new(Wave::new(3));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let release_handler = release.clone();
    let (failed, failure_observed) = mpsc::channel();
    let server = Server::new(move |request| {
        if request.path.contains("/blocks/") {
            let first = started_handler.lock().unwrap().insert(request.path.clone());
            if first && started_handler.lock().unwrap().len() <= 3 {
                first_wave.wait();
            }
            if request.method == "PUT" {
                if request.path == failing {
                    failed.send(()).unwrap();
                    return Reply::status(403);
                }
                let (lock, signal) = &*release_handler;
                let guard = lock.lock().unwrap();
                let (guard, timeout) = signal
                    .wait_timeout_while(guard, Duration::from_secs(5), |released| !*released)
                    .unwrap();
                assert!(*guard && !timeout.timed_out(), "inflight release timed out");
            }
        }
        remote.lock().unwrap().handle(request)
    });
    let publisher = publisher(&server, &[("OSM_UPLOAD_CONCURRENCY", "3")]);
    let directory = build.path().to_owned();
    let task = thread::spawn(move || {
        let mut progress = Vec::new();
        let result = publisher.publish(&directory, |event| progress.push(event));
        (result, progress)
    });
    failure_observed
        .recv_timeout(Duration::from_secs(3))
        .unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(
        !task.is_finished(),
        "publisher returned before draining inflight requests"
    );
    assert_eq!(started.lock().unwrap().len(), 3);
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    let (result, progress) = task.join().unwrap();
    assert!(result.unwrap_err().to_string().contains("R2 upload failed"));
    assert_eq!(started.lock().unwrap().len(), 3);
    assert_eq!(
        progress
            .iter()
            .filter(|event| event.stage == "upload")
            .map(|event| event.completed)
            .collect::<Vec<_>>(),
        [Some(0), Some(1), Some(2)]
    );
    assert!(
        !server
            .events()
            .iter()
            .any(|(method, path)| method == "POST" || path.contains("/manifests/"))
    );
    assert!(!build.path().join("publish-receipt.json").exists());
}

#[test]
fn r2_errors_do_not_expose_response_bodies_or_follow_redirects() {
    for method in ["HEAD", "PUT"] {
        let build = Build::new(1);
        let remote = Remote::new(&build);
        let destination = Server::new(|_| panic!("S3 redirect leaked credentials"));
        let target = destination.url.clone();
        let server = Server::new(move |request| {
            if request.method == method {
                Reply {
                    headers: vec![("Location".into(), target.clone())],
                    body: format!("{TOKEN} {SECRET}").into_bytes(),
                    ..Reply::status(302)
                }
            } else {
                remote.lock().unwrap().handle(request)
            }
        });
        let error = publisher(&server, &[])
            .publish(build.path(), |_| {})
            .unwrap_err()
            .to_string();
        assert!(!error.contains(TOKEN) && !error.contains(SECRET));
        assert!(error.starts_with("R2 "));
        assert!(destination.events().is_empty());
        assert_eq!(
            server
                .events()
                .iter()
                .filter(|(current, _)| current == method)
                .count(),
            1
        );
    }
}
