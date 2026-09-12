use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use aura_osm::config::{Environment, Runtime};

const DEFAULTS: &str = include_str!("../config/runtime.json");

fn environment(values: &[(&str, &str)]) -> Environment {
    Environment::from_values(
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    )
}

#[test]
fn settings_are_typed_and_overrides_do_not_mutate_other_runs() {
    let root = Path::new("/project/osm");
    let defaults = Runtime::from_json(root, DEFAULTS, &Environment::default()).unwrap();
    assert_eq!(defaults.compute_workers, 4);
    assert_eq!(defaults.compute_pending_batches, 4);
    assert_eq!(defaults.sqlite_cache_mib, 16);
    assert_eq!(defaults.data_dir, root.join(".build/data"));
    let adjusted = Runtime::from_json(
        root,
        DEFAULTS,
        &environment(&[
            ("OSM_COMPUTE_WORKERS", "3"),
            ("OSM_HTTP_TIMEOUT_MS", "125"),
            ("OSM_SQLITE_CACHE_MIB", "256"),
            ("OSM_DATA_DIR", "folder with spaces/../data"),
        ]),
    )
    .unwrap();
    assert_eq!(adjusted.compute_workers, 3);
    assert_eq!(adjusted.compute_pending_batches, 3);
    assert_eq!(adjusted.sqlite_cache_mib, 256);
    assert_eq!(adjusted.http_timeout, Duration::from_millis(125));
    assert_eq!(
        adjusted.data_dir.as_os_str(),
        "/project/osm/folder with spaces/../data"
    );
    let auto = Runtime::from_json(
        root,
        DEFAULTS,
        &environment(&[("OSM_COMPUTE_WORKERS", "auto")]),
    )
    .unwrap();
    assert_eq!(
        auto.compute_workers,
        std::thread::available_parallelism().unwrap().get()
    );
    assert_eq!(auto.compute_pending_batches, auto.compute_workers);
    assert_eq!(defaults.compute_workers, 4);
}

#[test]
fn invalid_runtime_values_and_legacy_names_fail_without_echoing_values() {
    for (key, value) in [
        ("OSM_COMPUTE_WORKERS", "0"),
        ("OSM_COMPUTE_WORKERS", "01"),
        ("OSM_COMPUTE_BATCH_BYTES", ""),
        ("OSM_COMPUTE_BATCH_SIZE", "-1"),
        ("OSM_UPLOAD_CONCURRENCY", "65"),
        ("OSM_HTTP_ATTEMPTS", "11"),
        ("OSM_SCHEDULE_DAY", "0"),
        ("OSM_SCHEDULE_HOUR", "24"),
        ("OSM_SCHEDULE_MINUTE", "60"),
        ("OSM_DATA_DIR", "~another-user/data"),
        ("OSM_DATA_DIR", " "),
        ("OSM_DIFF_MAX_MIB", "18446744073709551615"),
        ("OSM_SQLITE_CACHE_MIB", "999999999"),
    ] {
        assert!(
            Runtime::from_json(
                Path::new("/project"),
                DEFAULTS,
                &environment(&[(key, value)])
            )
            .is_err(),
            "{key}"
        );
    }
    let error = Runtime::from_json(
        Path::new("/project"),
        DEFAULTS,
        &environment(&[("OSM_BUILD_CONCURRENCY", "private-test-value")]),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("OSM_COMPUTE_WORKERS"));
    assert!(!error.contains("private-test-value"));
    assert!(
        Runtime::from_json(
            Path::new("/project"),
            DEFAULTS,
            &environment(&[
                ("OSM_HTTP_ATTEMPTS", "10"),
                ("OSM_HTTP_RETRY_DELAY_MS", "2147483647"),
            ])
        )
        .is_err()
    );
    let mut defaults: BTreeMap<String, serde_json::Value> = serde_json::from_str(DEFAULTS).unwrap();
    defaults.insert("OSM_UNKNOWN".into(), 1.into());
    assert!(
        Runtime::from_json(
            Path::new("/project"),
            &serde_json::to_string(&defaults).unwrap(),
            &Environment::default()
        )
        .is_err()
    );
}

#[test]
fn dotenv_preserves_credentials_as_text_and_supports_quoted_paths() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&tests).unwrap();
    let root = tempfile::tempdir_in(tests).unwrap();
    fs::write(
        root.path().join(".env"),
        concat!(
            "export AURA_OSM_TEST_LITERAL=\"$HOME $(touch forbidden) `whoami` # literal\"\n",
            "AURA_OSM_TEST_MULTILINE='first\nsecond'\n",
            "AURA_OSM_TEST_EMPTY=\n",
            "AURA_OSM_TEST_SPACES=unquoted text # comment\n",
            "AURA_OSM_TEST_ESCAPE=\"first\\nsecond\"\n",
            "AURA_OSM_TEST_SINGLE='first\\nsecond'\n",
        ),
    )
    .unwrap();
    let env = Environment::load(root.path()).unwrap();
    assert_eq!(
        env.get("AURA_OSM_TEST_LITERAL"),
        Some("$HOME $(touch forbidden) `whoami` # literal")
    );
    assert_eq!(env.get("AURA_OSM_TEST_MULTILINE"), Some("first\nsecond"));
    assert_eq!(env.get("AURA_OSM_TEST_EMPTY"), Some(""));
    assert_eq!(env.get("AURA_OSM_TEST_SPACES"), Some("unquoted text"));
    assert_eq!(env.get("AURA_OSM_TEST_ESCAPE"), Some("first\nsecond"));
    assert_eq!(env.get("AURA_OSM_TEST_SINGLE"), Some("first\\nsecond"));
    assert!(!root.path().join("forbidden").exists());
    let home = Runtime::from_json(
        root.path(),
        DEFAULTS,
        &environment(&[("HOME", "/users/test"), ("OSM_DATA_DIR", "~/data/../osm")]),
    )
    .unwrap();
    assert_eq!(home.data_dir.as_os_str(), "/users/test/data/../osm");
}
