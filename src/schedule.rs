use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail, ensure};
use plist::{Dictionary, Value};

use crate::{
    config::{Environment, Runtime},
    storage::atomic_write,
};

pub const LABEL: &str = "com.autheai.osm-update";
pub const WORKER_LABEL: &str = "com.autheai.osm-worker";
pub const SYSTEMD_UNIT_NAME: &str = "osm-worker.service";
pub const ROOT_POINTER_RELATIVE: &str = ".config/aura-osm/root";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployTarget {
    pub host: String,
    pub path: Option<String>,
}

pub fn worker_plist_definition(root: &Path, cleanup: bool) -> Result<Value> {
    let root = root.canonicalize()?;
    let text = |path: PathBuf| -> Result<Value> {
        Ok(Value::String(
            path.to_str()
                .context("Service paths must be valid UTF-8")?
                .to_owned(),
        ))
    };
    let mut arguments = vec![
        text(std::env::current_exe()?.canonicalize()?)?,
        Value::String("--root".into()),
        text(root.clone())?,
        Value::String("work".into()),
    ];
    if cleanup {
        arguments.push(Value::String("--cleanup".into()));
    }
    let mut definition = Dictionary::new();
    definition.insert("Label".into(), Value::String(WORKER_LABEL.into()));
    definition.insert("ProgramArguments".into(), Value::Array(arguments));
    definition.insert("WorkingDirectory".into(), text(root.clone())?);
    definition.insert("RunAtLoad".into(), Value::Boolean(true));
    definition.insert("KeepAlive".into(), Value::Boolean(true));
    definition.insert("ThrottleInterval".into(), Value::Integer(10.into()));
    definition.insert("ExitTimeOut".into(), Value::Integer(3600.into()));
    definition.insert("Umask".into(), Value::Integer(0o077.into()));
    definition.insert(
        "StandardOutPath".into(),
        text(root.join(".build/tools/logs/worker.log"))?,
    );
    definition.insert(
        "StandardErrorPath".into(),
        text(root.join(".build/tools/logs/worker.err.log"))?,
    );
    Ok(Value::Dictionary(definition))
}

fn systemd_quote(value: &str) -> Result<String> {
    ensure!(
        !value
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\n' | b'\r')),
        "Systemd paths must not contain control characters"
    );
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    Ok(format!("\"{escaped}\""))
}

pub fn worker_systemd_unit(root: &Path, executable: &Path, cleanup: bool) -> Result<String> {
    let root = root.canonicalize()?;
    let executable = executable.canonicalize()?;
    let root_text = root.to_str().context("Service paths must be valid UTF-8")?;
    let exe_text = executable
        .to_str()
        .context("Service paths must be valid UTF-8")?;
    let mut command = format!(
        "{} --root {} work",
        systemd_quote(exe_text)?,
        systemd_quote(root_text)?
    );
    if cleanup {
        command.push_str(" --cleanup");
    }
    Ok(format!(
        "[Unit]\n\
         Description=Aura OSM Regional Worker\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         WorkingDirectory={}\n\
         ExecStart={command}\n\
         Restart=always\n\
         RestartSec=10\n\
         TimeoutStopSec=3600\n\
         KillSignal=SIGTERM\n\
         UMask=0077\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        systemd_quote(root_text)?
    ))
}

pub fn install_worker(
    root: &Path,
    environment: &Environment,
    cleanup: bool,
    output: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = output {
        if path.extension().and_then(|ext| ext.to_str()) == Some("service") {
            let unit = worker_systemd_unit(root, &std::env::current_exe()?, cleanup)?;
            fs::create_dir_all(
                path.parent()
                    .context("Service output has no parent directory")?,
            )?;
            atomic_write(path, unit.as_bytes())?;
        } else {
            write_plist(path, &worker_plist_definition(root, cleanup)?)?;
        }
        return Ok(path.to_owned());
    }
    let home = environment
        .get("HOME")
        .context("HOME is required to install the worker service")?;
    let pointer = Path::new(home).join(ROOT_POINTER_RELATIVE);
    fs::create_dir_all(
        pointer
            .parent()
            .context("Root pointer path has no parent")?,
    )?;
    let canonical_root = root.canonicalize()?;
    let root_text = canonical_root
        .to_str()
        .context("Service paths must be valid UTF-8")?;
    atomic_write(&pointer, format!("{root_text}\n").as_bytes())?;
    if cfg!(target_os = "macos") {
        let path = Path::new(home)
            .join("Library/LaunchAgents")
            .join(format!("{WORKER_LABEL}.plist"));
        let definition = worker_plist_definition(root, cleanup)?;
        let uid = rustix::process::getuid().as_raw();
        fs::create_dir_all(root.join(".build/tools/logs"))?;
        write_plist(&path, &definition)?;
        let domain = format!("gui/{uid}");
        let target = format!("{domain}/{WORKER_LABEL}");
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
                "Cannot unload the existing worker service"
            );
        }
        ensure!(
            Command::new("/bin/launchctl")
                .args(["bootstrap", &domain])
                .arg(&path)
                .status()?
                .success(),
            "Cannot install the worker service"
        );
        return Ok(path);
    }
    ensure!(
        cfg!(target_os = "linux"),
        "Worker service installation requires macOS or Linux"
    );
    let path = Path::new(home)
        .join(".config/systemd/user")
        .join(SYSTEMD_UNIT_NAME);
    let unit = worker_systemd_unit(root, &std::env::current_exe()?, cleanup)?;
    fs::create_dir_all(
        path.parent()
            .context("Systemd user directory has no parent")?,
    )?;
    atomic_write(&path, unit.as_bytes())?;
    let uid = rustix::process::getuid().as_raw();
    let run_systemctl = |args: &[&str]| -> Result<bool> {
        let mut command = Command::new("systemctl");
        command.args(args);
        if environment.get("XDG_RUNTIME_DIR").is_none()
            && std::env::var_os("XDG_RUNTIME_DIR").is_none()
        {
            command.env("XDG_RUNTIME_DIR", format!("/run/user/{uid}"));
        }
        Ok(command.status()?.success())
    };
    ensure!(
        run_systemctl(&["--user", "daemon-reload"])?,
        "Cannot reload user systemd daemon"
    );
    ensure!(
        run_systemctl(&["--user", "enable", SYSTEMD_UNIT_NAME])?,
        "Cannot enable osm-worker.service"
    );
    ensure!(
        run_systemctl(&["--user", "restart", SYSTEMD_UNIT_NAME])?,
        "Cannot restart osm-worker.service"
    );
    Ok(path)
}

fn sh_quote(value: &str) -> Result<String> {
    ensure!(
        !value
            .bytes()
            .any(|byte| matches!(byte, b'\0' | b'\n' | b'\r')),
        "Deploy paths and hosts must not contain control characters"
    );
    Ok(format!("'{}'", value.replace('\'', "'\\''")))
}

pub fn parse_deploy_targets(
    explicit: &[String],
    environment: &Environment,
) -> Result<Vec<DeployTarget>> {
    let raw_entries: Vec<&str> = if !explicit.is_empty() {
        explicit.iter().map(String::as_str).collect()
    } else if let Some(configured) = environment.get("OSM_WORKER_HOSTS") {
        configured.split(',').collect()
    } else {
        bail!("Configure OSM_WORKER_HOSTS in .env or pass target hosts to osm deploy");
    };
    let mut targets = Vec::new();
    for entry in raw_entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        ensure!(
            !trimmed
                .bytes()
                .any(|byte| matches!(byte, b'\0' | b'\n' | b'\r')),
            "OSM_WORKER_HOSTS entries must not contain control characters"
        );
        let (host, path) = match trimmed.split_once(':') {
            Some((host, path)) => {
                let host = host.trim();
                let path = path.trim();
                ensure!(
                    !host.is_empty() && !path.is_empty(),
                    "Deploy target '{trimmed}' must have non-empty host and path"
                );
                (host.to_owned(), Some(path.to_owned()))
            }
            None => (trimmed.to_owned(), None),
        };
        ensure!(
            !host.starts_with('-') && !host.chars().any(char::is_whitespace),
            "Invalid SSH host '{host}'"
        );
        targets.push(DeployTarget { host, path });
    }
    ensure!(
        !targets.is_empty(),
        "Configure at least one host in OSM_WORKER_HOSTS"
    );
    Ok(targets)
}

pub fn remote_deploy_script(target: &DeployTarget, cleanup: bool) -> Result<String> {
    let directory = match target.path.as_deref() {
        Some("~") => "\"$HOME\"".to_owned(),
        Some(path) if path.starts_with("~/") => {
            format!("\"$HOME\"/{}", sh_quote(&path[2..])?)
        }
        Some(path) => sh_quote(path)?,
        None => format!("\"$(tr -d '\\r\\n' < \"$HOME/{ROOT_POINTER_RELATIVE}\")\""),
    };
    let service_flag = if cleanup { " --cleanup" } else { "" };
    Ok(format!(
        "export PATH=\"$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH\" && cd {directory} && git pull --ff-only && cargo build --release && .build/tools/rust/release/osm service{service_flag}"
    ))
}

pub fn deploy_workers(
    explicit: &[String],
    environment: &Environment,
    cleanup: bool,
) -> Result<Vec<DeployTarget>> {
    let targets = parse_deploy_targets(explicit, environment)?;
    for target in &targets {
        let script = remote_deploy_script(target, cleanup)?;
        let status = Command::new("ssh")
            .arg("--")
            .arg(&target.host)
            .arg(&script)
            .status()
            .with_context(|| format!("Cannot start ssh for {}", target.host))?;
        ensure!(status.success(), "Deployment failed on {}", target.host);
    }
    Ok(targets)
}

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
            Value::String("--replace".into()),
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
        assert_eq!(arguments.len(), 7);
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
        assert_eq!(arguments[6].as_string(), Some("--replace"));
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

    #[test]
    fn worker_plist_and_systemd_unit_keep_worker_alive_with_graceful_drain_timeout() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let plist = worker_plist_definition(root, true).unwrap();
        let plist = plist.as_dictionary().unwrap();
        assert_eq!(plist["Label"].as_string(), Some(WORKER_LABEL));
        assert_eq!(plist["RunAtLoad"].as_boolean(), Some(true));
        assert_eq!(plist["KeepAlive"].as_boolean(), Some(true));
        assert_eq!(plist["ThrottleInterval"].as_unsigned_integer(), Some(10));
        assert_eq!(plist["ExitTimeOut"].as_unsigned_integer(), Some(3600));
        assert_eq!(plist["Umask"].as_unsigned_integer(), Some(0o077));
        assert!(!plist.contains_key("EnvironmentVariables"));
        let arguments = plist["ProgramArguments"].as_array().unwrap();
        assert_eq!(arguments[1].as_string(), Some("--root"));
        assert_eq!(arguments[3].as_string(), Some("work"));
        assert_eq!(arguments[4].as_string(), Some("--cleanup"));

        let exe = std::env::current_exe().unwrap();
        let unit = worker_systemd_unit(root, &exe, true).unwrap();
        assert!(unit.contains("Restart=always\n"));
        assert!(unit.contains("RestartSec=10\n"));
        assert!(unit.contains("TimeoutStopSec=3600\n"));
        assert!(unit.contains("KillSignal=SIGTERM\n"));
        assert!(unit.contains("UMask=0077\n"));
        assert!(unit.contains(" work --cleanup\n"));
    }

    #[test]
    fn deploy_targets_support_per_host_paths_and_root_pointer_fallback() {
        let environment = Environment::from_values(std::collections::BTreeMap::from([(
            "OSM_WORKER_HOSTS".into(),
            "mac-mini:/Users/dev/Aura/osm-builder, debian:~/work/osm-builder, fallback-host".into(),
        )]));
        let targets = parse_deploy_targets(&[], &environment).unwrap();
        assert_eq!(
            targets,
            vec![
                DeployTarget {
                    host: "mac-mini".into(),
                    path: Some("/Users/dev/Aura/osm-builder".into()),
                },
                DeployTarget {
                    host: "debian".into(),
                    path: Some("~/work/osm-builder".into()),
                },
                DeployTarget {
                    host: "fallback-host".into(),
                    path: None,
                },
            ]
        );
        let mac_script = remote_deploy_script(&targets[0], true).unwrap();
        assert!(mac_script.contains("cd '/Users/dev/Aura/osm-builder' && git pull --ff-only && cargo build --release && .build/tools/rust/release/osm service --cleanup"));
        let debian_script = remote_deploy_script(&targets[1], true).unwrap();
        assert!(debian_script.contains("cd \"$HOME\"/'work/osm-builder' && git pull --ff-only"));
        let pointer_script = remote_deploy_script(&targets[2], true).unwrap();
        assert!(pointer_script.contains(".config/aura-osm/root"));
    }
}
