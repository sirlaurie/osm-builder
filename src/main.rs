use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

use anyhow::{Context, Result, bail, ensure};
use aura_osm::{
    build,
    config::{Environment, Runtime},
    format, incremental, pipeline,
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
        region: String,
        #[arg(
            long,
            help = "Remove each region's local data after confirmed publication"
        )]
        cleanup: bool,
    },
    #[command(about = "Apply daily changes and publish; requires existing indexes")]
    Update { region: String },
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
    let _lock = match cli.command {
        Action::List | Action::PublishedState | Action::Schedule { .. } => None,
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
            &region,
            job.as_deref(),
            false,
        )?,
        Action::Bootstrap { region, cleanup } => pipeline::run(
            &root,
            &runtime,
            &environment,
            "bootstrap",
            &region,
            None,
            cleanup,
        )?,
        Action::Update { region } => pipeline::run(
            &root,
            &runtime,
            &environment,
            "update",
            &region,
            None,
            false,
        )?,
        Action::Publish { directory } => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            let mut progress = ProgressReporter::default();
            let receipt = publisher.publish(&directory, |event| progress.update(event))?;
            drop(progress);
            print_json(&receipt)?;
        }
        Action::PublishedState => {
            let publisher =
                Publisher::new(&runtime, &PublishConfig::from_environment(&environment)?)?;
            print_json(&serde_json::to_value(publisher.read_state()?)?)?;
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
