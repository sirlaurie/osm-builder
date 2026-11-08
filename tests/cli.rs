use std::{
    fs,
    fs::OpenOptions,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::{Command, Output},
};

fn invoke(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_osm"))
        .current_dir(root.parent().unwrap())
        .env_clear()
        .env("OSM_COMPUTE_WORKERS", "1")
        .arg("--root")
        .arg(root)
        .args(arguments)
        .output()
        .unwrap()
}

#[test]
fn cli_loads_project_through_directory_symlinks_and_holds_an_os_lock() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&tests).unwrap();
    let work = tempfile::tempdir_in(tests).unwrap();
    let project = work.path().join("project with spaces");
    fs::create_dir_all(project.join("config")).unwrap();
    fs::write(
        project.join("config/runtime.json"),
        include_str!("../config/runtime.json"),
    )
    .unwrap();
    fs::write(
        project.join("config/regions.json"),
        r#"{"regions":[{"id":"cn-jiangsu","extract":"asia/china/jiangsu"}]}"#,
    )
    .unwrap();
    fs::write(
        project.join(".env"),
        "OSM_COMPUTE_WORKERS=invalid\nOSM_PUBLISH_TOKEN='$(touch forbidden)'\n",
    )
    .unwrap();
    let linked = work.path().join("linked project");
    symlink(&project, &linked).unwrap();
    let listed = invoke(&linked, &["list"]);
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    assert_eq!(
        String::from_utf8(listed.stdout).unwrap(),
        "cn-jiangsu\tasia/china/jiangsu\n"
    );
    assert!(project.join(".build/tools/tmp").is_dir());
    assert_eq!(
        fs::metadata(project.join(".build/tools/tmp"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert!(!work.path().join("forbidden").exists());
    let plist = work.path().join("monthly update.plist");
    let scheduled = invoke(&linked, &["schedule", "--output", plist.to_str().unwrap()]);
    assert!(
        scheduled.status.success(),
        "{}",
        String::from_utf8_lossy(&scheduled.stderr)
    );
    let definition = plist::Value::from_file(&plist).unwrap();
    let definition = definition.as_dictionary().unwrap();
    let arguments = definition["ProgramArguments"].as_array().unwrap();
    assert_eq!(
        arguments[0].as_string(),
        Path::new(env!("CARGO_BIN_EXE_osm"))
            .canonicalize()
            .unwrap()
            .to_str()
    );
    assert_eq!(arguments[1].as_string(), Some("--root"));
    assert_eq!(
        arguments[2].as_string(),
        project.canonicalize().unwrap().to_str()
    );
    assert_eq!(arguments[3].as_string(), Some("update"));
    assert_eq!(arguments[4].as_string(), Some("all"));
    let worker_plist = work.path().join("worker.plist");
    let worker_scheduled = invoke(
        &linked,
        &[
            "service",
            "--cleanup",
            "--output",
            worker_plist.to_str().unwrap(),
        ],
    );
    assert!(
        worker_scheduled.status.success(),
        "{}",
        String::from_utf8_lossy(&worker_scheduled.stderr)
    );
    let worker_def = plist::Value::from_file(&worker_plist).unwrap();
    let worker_def = worker_def.as_dictionary().unwrap();
    assert_eq!(worker_def["KeepAlive"].as_boolean(), Some(true));
    assert_eq!(worker_def["ExitTimeOut"].as_unsigned_integer(), Some(3600));
    let worker_args = worker_def["ProgramArguments"].as_array().unwrap();
    assert_eq!(worker_args[3].as_string(), Some("work"));
    assert_eq!(worker_args[4].as_string(), Some("--cleanup"));

    let worker_unit = work.path().join("osm-worker.service");
    let unit_scheduled = invoke(
        &linked,
        &[
            "service",
            "--cleanup",
            "--output",
            worker_unit.to_str().unwrap(),
        ],
    );
    assert!(
        unit_scheduled.status.success(),
        "{}",
        String::from_utf8_lossy(&unit_scheduled.stderr)
    );
    let unit_text = fs::read_to_string(&worker_unit).unwrap();
    assert!(unit_text.contains(&format!(
        "WorkingDirectory={}\n",
        project.canonicalize().unwrap().to_str().unwrap()
    )));
    assert!(unit_text.contains("Restart=always\n"));
    assert!(unit_text.contains("TimeoutStopSec=3600\n"));
    assert!(unit_text.contains(" work --cleanup\n"));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(project.join(".build/pipeline.lock"))
        .unwrap();
    lock.lock().unwrap();
    let blocked = invoke(&linked, &["init", "cn-jiangsu"]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("Another OSM pipeline is running"));
    drop(lock);
    let unlocked = invoke(&linked, &["status", "--state", "missing-state"]);
    assert!(!unlocked.status.success());
    assert!(!String::from_utf8_lossy(&unlocked.stderr).contains("Another OSM pipeline is running"));
    assert!(project.join(".build/pipeline.lock").is_file());
}
