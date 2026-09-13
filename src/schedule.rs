use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, ensure};
use plist::{Dictionary, Value};

use crate::{
    config::{Environment, Runtime},
    storage::atomic_write,
};

pub const LABEL: &str = "com.autheai.osm-update";

pub fn job_definition(root: &Path, runtime: &Runtime) -> Result<Value> {
    let root = root.canonicalize()?;
    let text = |path: PathBuf| -> Result<Value> {
        Ok(Value::String(
            path.to_str()
                .context("Schedule paths must be valid UTF-8")?
                .to_owned(),
        ))
    };
    let mut calendar = Dictionary::new();
    calendar.insert("Day".into(), Value::Integer(runtime.schedule_day.into()));
    calendar.insert("Hour".into(), Value::Integer(runtime.schedule_hour.into()));
    calendar.insert(
        "Minute".into(),
        Value::Integer(runtime.schedule_minute.into()),
    );
    let mut definition = Dictionary::new();
    definition.insert("Label".into(), Value::String(LABEL.into()));
    definition.insert(
        "ProgramArguments".into(),
        Value::Array(vec![
            text(std::env::current_exe()?.canonicalize()?)?,
            Value::String("--root".into()),
            text(root.clone())?,
            Value::String("update".into()),
            Value::String("all".into()),
            Value::String("--submit-only".into()),
        ]),
    );
    definition.insert("WorkingDirectory".into(), text(root.clone())?);
    definition.insert("StartCalendarInterval".into(), Value::Dictionary(calendar));
    definition.insert("RunAtLoad".into(), Value::Boolean(false));
    definition.insert("Umask".into(), Value::Integer(0o077.into()));
    definition.insert(
        "StandardOutPath".into(),
        text(root.join(".build/tools/logs/monthly-update.log"))?,
    );
    definition.insert(
        "StandardErrorPath".into(),
        text(root.join(".build/tools/logs/monthly-update.err.log"))?,
    );
    Ok(Value::Dictionary(definition))
}

pub fn write_plist(path: &Path, definition: &Value) -> Result<()> {
    let mut payload = Vec::new();
    definition.to_writer_xml(&mut payload)?;
    fs::create_dir_all(
        path.parent()
            .context("Schedule output has no parent directory")?,
    )?;
    atomic_write(path, &payload)
}

pub fn run(
    root: &Path,
    runtime: &Runtime,
    environment: &Environment,
    output: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = output {
        write_plist(path, &job_definition(root, runtime)?)?;
        return Ok(path.to_owned());
    }
    ensure!(
        cfg!(target_os = "macos"),
        "Monthly scheduling requires macOS"
    );
    let home = environment
        .get("HOME")
        .context("HOME is required to install the monthly schedule")?;
    let path = Path::new(home)
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"));
    let definition = job_definition(root, runtime)?;
    let uid = rustix::process::getuid().as_raw();
    fs::create_dir_all(root.join(".build/tools/logs"))?;
    write_plist(&path, &definition)?;
    let domain = format!("gui/{uid}");
    let target = format!("{domain}/{LABEL}");
    let loaded = Command::new("/bin/launchctl")
        .args(["print", &target])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if loaded.success() {
        ensure!(
            Command::new("/bin/launchctl")
                .args(["bootout", &target])
                .status()?
                .success(),
            "Cannot unload the existing monthly schedule"
        );
    }
    ensure!(
        Command::new("/bin/launchctl")
            .args(["bootstrap", &domain])
            .arg(&path)
            .status()?
            .success(),
        "Cannot install the monthly schedule"
    );
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_uses_runtime_calendar_and_reloads_files_at_each_run() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let environment = Environment::from_values(std::collections::BTreeMap::from([
            ("OSM_SCHEDULE_DAY".into(), "14".into()),
            ("OSM_SCHEDULE_HOUR".into(), "9".into()),
            ("OSM_SCHEDULE_MINUTE".into(), "35".into()),
            ("OSM_ADMIN_TOKEN".into(), "must-not-be-written".into()),
            ("OSM_COMPUTE_WORKERS".into(), "3".into()),
        ]));
        let runtime = Runtime::load(root, &environment).unwrap();
        let definition = job_definition(root, &runtime).unwrap();
        let definition = definition.as_dictionary().unwrap();
        let calendar = definition["StartCalendarInterval"].as_dictionary().unwrap();
        assert_eq!(calendar["Day"].as_unsigned_integer(), Some(14));
        assert_eq!(calendar["Hour"].as_unsigned_integer(), Some(9));
        assert_eq!(calendar["Minute"].as_unsigned_integer(), Some(35));
        assert_eq!(definition["RunAtLoad"].as_boolean(), Some(false));
        assert!(!definition.contains_key("EnvironmentVariables"));
        assert!(!definition.contains_key("KeepAlive"));
        let arguments = definition["ProgramArguments"].as_array().unwrap();
        assert_eq!(arguments.len(), 6);
        assert_eq!(
            arguments[0].as_string(),
            std::env::current_exe()
                .unwrap()
                .canonicalize()
                .unwrap()
                .to_str()
        );
        assert_eq!(arguments[1].as_string(), Some("--root"));
        assert_eq!(
            arguments[2].as_string(),
            root.canonicalize().unwrap().to_str()
        );
        assert_eq!(arguments[3].as_string(), Some("update"));
        assert_eq!(arguments[4].as_string(), Some("all"));
        assert_eq!(arguments[5].as_string(), Some("--submit-only"));
        assert_eq!(definition["Umask"].as_unsigned_integer(), Some(0o077));
        let mut encoded = Vec::new();
        Value::Dictionary(definition.clone())
            .to_writer_xml(&mut encoded)
            .unwrap();
        let encoded = String::from_utf8(encoded).unwrap();
        assert!(!encoded.contains("must-not-be-written"));
        assert!(!encoded.contains("OSM_COMPUTE_WORKERS"));
    }

    #[test]
    fn atomic_plist_preserves_spaces_and_does_not_store_environment() {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tests");
        fs::create_dir_all(&scratch).unwrap();
        let work = tempfile::Builder::new()
            .prefix("schedule-")
            .tempdir_in(scratch)
            .unwrap();
        let path = work.path().join("folder with spaces/job.plist");
        let mut definition = Dictionary::new();
        definition.insert("Label".into(), Value::String(LABEL.into()));
        definition.insert(
            "ProgramArguments".into(),
            Value::Array(vec![
                Value::String("/path with spaces/osm".into()),
                Value::String("--root".into()),
                Value::String("/project with spaces".into()),
                Value::String("update".into()),
                Value::String("all".into()),
            ]),
        );
        write_plist(&path, &Value::Dictionary(definition.clone())).unwrap();
        let loaded = Value::from_file(&path).unwrap();
        assert_eq!(loaded, Value::Dictionary(definition));
        assert!(
            loaded
                .as_dictionary()
                .unwrap()
                .get("EnvironmentVariables")
                .is_none()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
