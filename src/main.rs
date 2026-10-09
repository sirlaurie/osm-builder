use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use aura_osm::{
    build,
    config::{Environment, Runtime},
    dispatch, format, gc, incremental, network, pipeline,
    publish::{ProgressReporter, PublishConfig, Publisher},
    schedule, source,
};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "osm",
    version,
    about = "Build, update and publish Aura regional OSM data"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "OSM project directory containing config/ and .env"
    )]
    root: Option<PathBuf>,
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    #[command(about = "List configured regions")]
    List,
    #[command(about = "Download and compile one region without publishing")]
    Build { region: String },
    #[command(about = "Compile an existing downloaded job into a new directory")]
    Rebuild { job: PathBuf },
    #[command(about = "Create a persistent index without publishing")]
    Init {
        region: String,
        job: Option<PathBuf>,
    },
    #[command(about = "Initialize, update and publish regions")]
    Bootstrap {
        #[arg(required = true)]
        regions: Vec<String>,
        #[arg(long, help = "Submit the batch without claiming tasks on this process")]
        submit_only: bool,
        #[arg(
            long,
            requires = "submit_only",
            help = "Cancel an unfinished batch before submitting"
        )]
        replace: bool,
        #[arg(
            long,
            help = "Remove each region's local data after confirmed publication"
        )]
        cleanup: bool,
    },
    #[command(about = "Submit regional updates and process the batch")]
    Update {
        #[arg(required = true)]
        regions: Vec<String>,
        #[arg(long, help = "Submit the batch without claiming tasks on this process")]
        submit_only: bool,
        #[arg(
            long,
            requires = "submit_only",
            help = "Cancel an unfinished batch before submitting"
        )]
        replace: bool,
    },
    #[command(about = "Cancel the unfinished cloud batch")]
    Cancel,
    #[command(about = "Remove regions from cloud publication")]
    Retire {
        #[arg(required = true)]
        regions: Vec<String>,
    },
    #[command(about = "Report or delete R2 objects that no published region references")]
    Gc {
        #[arg(
            long,
            help = "Delete the unreferenced objects instead of reporting them"
        )]
        apply: bool,
    },
    #[command(about = "Claim and process cloud regional tasks")]
    Work {
        #[arg(
            long,
            help = "Exit when the current batch has no pending or running tasks"
        )]
        once: bool,
        #[arg(long, help = "Remove local region data after confirmed publication")]
        cleanup: bool,
    },
    #[command(about = "Read cloud batch and device status")]
    Jobs,
    #[command(about = "Validate and publish a completed build")]
    Publish { directory: PathBuf },
    #[command(about = "Read verified cloud publication state")]
    PublishedState,
    #[command(about = "Install the monthly update task on this Mac")]
    Schedule {
        #[arg(long, help = "Write a plist for inspection without installing it")]
        output: Option<PathBuf>,
    },
    #[command(about = "Compile a local PBF snapshot without network access")]
    Compile {
        #[command(flatten)]
        source: SourceArguments,
        #[arg(long)]
        output: PathBuf,
    },
    #[command(about = "Create an incremental index from a local PBF snapshot")]
    InitIndex {
        #[command(flatten)]
        source: SourceArguments,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        replication_url: String,
    },
    #[command(about = "Apply one local OSC change file to an index")]
    Apply {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        sequence: u64,
        #[arg(long)]
        timestamp: String,
        #[arg(long)]
        coverage: Option<PathBuf>,
    },
    #[command(about = "Read the committed index state and restore its exported receipt")]
    Status {
        #[arg(long)]
        state: PathBuf,
    },
}

#[derive(Args)]
struct SourceArguments {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    coverage: PathBuf,
    #[arg(long)]
    region: String,
    #[arg(long)]
    source_timestamp: String,
    #[arg(long)]
    source_sequence: Option<u64>,
    #[arg(long)]
    source_sha256: String,
}

fn main() -> ExitCode {
    rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("OSM failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    let root = cli
        .root
        .unwrap_or(env::current_dir()?)
        .canonicalize()
        .context("Cannot locate OSM project directory")?;
    ensure!(
        root.join("config/runtime.json").is_file(),
        "Project directory has no config/runtime.json"
    );
    configure_temporary_directory(&root)?;
    let environment = Environment::load(&root)?;
    let runtime = Runtime::load(&root, &environment)?;
    match &cli.command {
        Action::Bootstrap {
            regions,
            cleanup,
            submit_only: true,
            replace,
        } => {
            ensure!(
                !cleanup,
                "--cleanup applies to processing; use work --cleanup on processing devices"
            );
            println!(
                "Submitted batch: {}",
                pipeline::submit(
                    &root,
                    &runtime,
                    &environment,
                    "bootstrap",
                    regions,
                    *replace
                )?
            );
            return Ok(());
        }
        Action::Update {
            regions,
            submit_only: true,
            replace,
        } => {
            println!(
                "Submitted batch: {}",
                pipeline::submit(&root, &runtime, &environment, "update", regions, *replace)?
            );
            return Ok(());
        }
        _ => {}
    }
    let _lock = match cli.command {
        Action::List
        | Action::Jobs
        | Action::PublishedState
        | Action::Schedule { .. }
        | Action::Cancel
        | Action::Retire { .. }
        | Action::Gc { .. } => None,
        _ => Some(lock_pipeline(&root)?),
    };
    match cli.command {
        Action::List => {
            for entry in source::regions(&root.join("config/regions.json"))? {
                println!("{}\t{}", entry.id, entry.extract);
            }
        }
        Action::Build { region } => {
            let output = pipeline::build(&root, &runtime, &region, None)?;
            println!("Build complete: {}", output.display());
        }
        Action::Rebuild { job } => {
            let job = job.canonicalize().context("Cannot open downloaded job")?;
            let url =
                fs::read_to_string(job.join("source.url")).context("Job has no source.url")?;
            let entry = source::regions(&root.join("config/regions.json"))?
                .into_iter()
                .find(|entry| entry.source_url() == url.trim())
                .context("Job is not from a configured regional source")?;
            let output = pipeline::build(&root, &runtime, &entry.id, Some(&job))?;
            println!("Build complete: {}", output.display());
        }
        Action::Init { region, job } => pipeline::run(
            &root,
            &runtime,
            &environment,
            "init",
            std::slice::from_ref(&region),
            job.as_deref(),
            false,
        )?,
        Action::Bootstrap {
            regions, cleanup, ..
        } => pipeline::run(
            &root,
            &runtime,
            &environment,
            "bootstrap",
            &regions,
            None,
            cleanup,
        )?,
        Action::Update { regions, .. } => pipeline::run(
            &root,
            &runtime,
            &environment,
            "update",
            &regions,
            None,
            false,
        )?,
        Action::Publish { directory } => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            let release = format::validate_release(
                &aura_osm::storage::read_local(
                    &directory.canonicalize()?,
                    "release.json",
                    format::MAX_CURRENT,
                    None,
                )?
                .value,
            )?;
            let entry = source::regions(&root.join("config/regions.json"))?
                .into_iter()
                .find(|entry| entry.id == release.region)
                .context("Build region is not configured")?;
            let client = publisher.coordinator();
            let batch = client.start("update", std::slice::from_ref(&entry))?;
            let device = dispatch::device_id(&runtime.data_dir)?;
            let mut previous = None;
            let receipt = network::retry(Duration::from_secs(60), || {
                let jobs = client.jobs()?;
                let job = publication_job(&jobs, &batch, &entry.id, &entry.extract)?;
                if job["status"] == "completed" {
                    let lease = previous.as_ref().context(
                        "Publication task completed before this process acquired a lease",
                    )?;
                    completed_publication(job, lease, &release.manifest)?;
                    let mut progress = ProgressReporter::for_region(&lease.region);
                    return publisher.publish(&directory, lease, |event| progress.update(event));
                }
                ensure!(
                    matches!(job["status"].as_str(), Some("pending" | "running")),
                    "Publication task is no longer available"
                );
                let claim = client.claim(&device, std::slice::from_ref(&entry.id), 0)?;
                let lease = claim.lease.ok_or_else(|| {
                    network::Transient("Waiting for the regional publication lease".into())
                })?;
                if lease.batch_id != batch
                    || lease.region != entry.id
                    || lease.extract != entry.extract
                {
                    let error = anyhow::anyhow!(
                        "Publication claim does not match the submitted regional task"
                    );
                    let _ = client.release(&lease, dispatch::ReleaseOutcome::Retry, &error);
                    return Err(error);
                }
                previous = Some(lease.clone());
                let result = (|| {
                    let guard = dispatch::Guard::start(client.clone(), lease.clone())?;
                    let mut progress = ProgressReporter::for_region(&lease.region);
                    publisher.publish(&directory, &guard.lease()?, |event| progress.update(event))
                })();
                if let Err(error) = &result {
                    let outcome = if network::retryable(error) {
                        dispatch::ReleaseOutcome::Retry
                    } else {
                        dispatch::ReleaseOutcome::Failed
                    };
                    if let Err(release_error) = client.release(&lease, outcome, error) {
                        eprintln!("Could not release publication task: {release_error:#}");
                    }
                }
                result
            })?;
            print_json(&receipt)?;
        }
        Action::Work { once, cleanup } => {
            pipeline::work(&root, &runtime, &environment, once, cleanup)?
        }
        Action::Jobs => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            print_json(&network::retry(Duration::from_secs(60), || {
                publisher.coordinator().jobs()
            })?)?;
        }
        Action::Cancel => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            match network::retry(Duration::from_secs(60), || {
                publisher.coordinator().cancel_active()
            })? {
                Some(result) => print_json(&result)?,
                None => println!("No unfinished batch"),
            }
        }
        Action::Retire { regions } => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            print_json(&network::retry(Duration::from_secs(60), || {
                publisher.coordinator().retire(&regions)
            })?)?;
        }
        Action::Gc { apply } => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            print_json(&gc::run(&publisher, apply)?)?;
        }
        Action::PublishedState => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            print_json(&serde_json::to_value(network::retry(
                Duration::from_secs(60),
                || publisher.read_state(),
            )?)?)?;
        }
        Action::Schedule { output } => {
            let path = schedule::run(&root, &runtime, &environment, output.as_deref())?;
            println!("Monthly schedule: {}", path.display());
        }
        Action::Compile { source, output } => {
            let receipt = build::run(
                &build::BuildOptions {
                    input: source.input,
                    coverage: source.coverage,
                    region: source.region,
                    source_timestamp: source.source_timestamp,
                    source_sequence: source.source_sequence,
                    source_sha256: source.source_sha256,
                    output,
                    scratch: root.join(".build/tools/tmp"),
                },
                &runtime.compute_options(),
            )?;
            print_json(&receipt)?;
        }
        Action::InitIndex {
            source,
            state,
            replication_url,
        } => {
            let receipt = incremental::initialize(
                &incremental::InitOptions {
                    input: source.input,
                    state,
                    coverage: source.coverage,
                    region: source.region,
                    source_timestamp: source.source_timestamp,
                    source_sequence: source
                        .source_sequence
                        .context("--source-sequence is required for init-index")?,
                    source_sha256: source.source_sha256,
                    replication_url,
                },
                &runtime.compute_options(),
            )?;
            print_json(&receipt)?;
        }
        Action::Apply {
            input,
            state,
            sequence,
            timestamp,
            coverage,
        } => {
            print_json(&incremental::apply_diff(
                &incremental::ApplyOptions {
                    input,
                    state,
                    sequence,
                    timestamp,
                    coverage,
                },
                &runtime.compute_options(),
            )?)?;
        }
        Action::Status { state } => {
            print_json(&incremental::status(&state, &runtime.compute_options())?)?
        }
    }
    Ok(())
}

fn publication_job<'a>(
    jobs: &'a serde_json::Value,
    batch: &str,
    region: &str,
    extract: &str,
) -> Result<&'a serde_json::Value> {
    ensure!(
        jobs["batchId"].as_str() == Some(batch),
        "Publication batch is no longer active; retaining the completed build"
    );
    let jobs = jobs["jobs"]
        .as_array()
        .context("Invalid dispatch job status")?;
    let matches = jobs
        .iter()
        .filter(|job| job["region"].as_str() == Some(region))
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "Publication task is missing or duplicated"
    );
    let job = matches[0];
    ensure!(
        job["extract"].as_str() == Some(extract),
        "Publication task source changed"
    );
    Ok(job)
}

fn completed_publication(
    job: &serde_json::Value,
    lease: &dispatch::Lease,
    manifest: &str,
) -> Result<()> {
    ensure!(
        job["status"] == "completed"
            && job["generation"].as_u64() == Some(lease.generation)
            && job["deviceId"].as_str() == Some(&lease.device_id)
            && job["slot"].as_u64() == Some(u64::from(lease.slot))
            && job["manifest"].as_str() == Some(manifest),
        "Publication completed under another lease or manifest; retaining the local build"
    );
    Ok(())
}

fn configure_temporary_directory(root: &Path) -> Result<()> {
    let directory = root.join(".build/tools/tmp");
    fs::create_dir_all(&directory)?;
    if env::var_os("SQLITE_TMPDIR").as_deref() != Some(directory.as_os_str())
        || env::var_os("TMPDIR").as_deref() != Some(directory.as_os_str())
    {
        let error = Command::new(env::current_exe()?)
            .args(env::args_os().skip(1))
            .env("SQLITE_TMPDIR", &directory)
            .env("TMPDIR", &directory)
            .exec();
        return Err(error).context("Cannot configure OSM temporary directory");
    }
    Ok(())
}

fn lock_pipeline(root: &Path) -> Result<File> {
    let path = root.join(".build/pipeline.lock");
    if path.is_dir() {
        bail!("An earlier pipeline owns .build/pipeline.lock; wait for it to finish");
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.try_lock().context("Another OSM pipeline is running")?;
    Ok(file)
}

fn print_json(value: &serde_json::Value) -> Result<()> {
    let mut output = std::io::stdout().lock();
    output.write_all(&format::canonical_json(value)?)?;
    output.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lease() -> dispatch::Lease {
        dispatch::Lease {
            batch_id: "11111111-1111-4111-8111-111111111111".into(),
            region: "configured-region".into(),
            extract: "europe/configured-region".into(),
            device_id: "22222222-2222-4222-8222-222222222222".into(),
            slot: 0,
            token: "33333333-3333-4333-8333-333333333333".into(),
            generation: 2,
            expires_at: "2026-01-01T00:00:00Z".into(),
            renew_after_seconds: 60,
        }
    }

    #[test]
    fn completed_publication_requires_the_attempted_lease_and_manifest_before_ack_replay() {
        let lease = lease();
        let manifest = "a".repeat(64);
        let job = json!({
            "region":lease.region,
            "extract":lease.extract,
            "status":"completed",
            "generation":lease.generation,
            "deviceId":lease.device_id,
            "slot":lease.slot,
            "manifest":manifest,
        });
        completed_publication(&job, &lease, &manifest).unwrap();
        for (field, value) in [
            ("status", json!("running")),
            ("generation", json!(3)),
            ("deviceId", json!("44444444-4444-4444-8444-444444444444")),
            ("slot", json!(1)),
            ("manifest", json!("b".repeat(64))),
        ] {
            let mut other = job.clone();
            other[field] = value;
            assert!(
                completed_publication(&other, &lease, &manifest).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn publication_recovery_cannot_follow_a_replacement_batch_or_changed_region_source() {
        let lease = lease();
        let job = json!({
            "region":lease.region,
            "extract":lease.extract,
            "status":"pending",
        });
        let jobs = json!({"batchId":lease.batch_id,"jobs":[job]});
        publication_job(&jobs, &lease.batch_id, &lease.region, &lease.extract).unwrap();
        for invalid in [
            json!({"batchId":"44444444-4444-4444-8444-444444444444","jobs":[job]}),
            json!({"batchId":lease.batch_id,"jobs":[]}),
            json!({"batchId":lease.batch_id,"jobs":[job,job]}),
            json!({"batchId":lease.batch_id,"jobs":[{"region":lease.region,"extract":"europe/other"}]}),
        ] {
            assert!(
                publication_job(&invalid, &lease.batch_id, &lease.region, &lease.extract).is_err()
            );
        }
    }
}
