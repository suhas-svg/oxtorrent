//! Argument parsing and progress output.

use std::collections::HashSet;
use std::io::{IsTerminal, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{ArgAction, Parser, Subcommand};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracing_subscriber::EnvFilter;

use crate::dht::{Dht, DhtConfig};
use crate::magnet::{self, FetchConfig, Magnet, MagnetError};
use crate::metainfo::Torrent;
use crate::peer::generate_peer_id;
use crate::ratelimit::Limits;
use crate::router::Router;
use crate::session::{self, Config, Inbound, Outcome, Progress};
use crate::sim;

#[derive(Parser, Debug)]
#[command(
    name = "oxtorrent",
    version,
    about = "A command-line BitTorrent client"
)]
pub struct Cli {
    /// More detailed logs (-v info, -vv debug, -vvv trace).
    #[arg(short, long, action = ArgAction::Count, global = true)]
    verbose: u8,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Print what a .torrent file describes.
    Info { torrent: PathBuf },
    /// Download a torrent from a .torrent file or a magnet link.
    Download(DownloadArgs),
    /// Run the swarm simulator on a scenario file.
    Simulate(sim::SimulateArgs),
}

#[derive(clap::Args, Debug)]
struct DownloadArgs {
    /// Paths to .torrent files or magnet links (quote them in the shell).
    /// Several are downloaded at once, sharing the port and the DHT.
    #[arg(required = true, value_name = "TORRENT")]
    torrents: Vec<String>,
    /// Where to save the files.
    #[arg(long, default_value = ".")]
    output: PathBuf,
    /// TCP port for incoming peers.
    #[arg(long, default_value_t = 6881)]
    port: u16,
    /// Maximum simultaneous connections, shared evenly between torrents.
    #[arg(long, default_value_t = 50)]
    max_peers: usize,
    /// Keep uploading after the download completes (until Ctrl-C).
    #[arg(long)]
    seed: bool,
    /// Minimum outstanding block requests per peer; the queue grows
    /// with each peer's measured speed.
    #[arg(long, default_value_t = 5)]
    pipeline: usize,
    /// Extra peer to connect to (ip:port); may be repeated.
    #[arg(long = "peer")]
    peers: Vec<SocketAddr>,
    /// Do not use the DHT to find peers (trackers and --peer only).
    #[arg(long)]
    no_dht: bool,
    /// Download speed limit for the whole run, in bytes per second
    /// (e.g. 500K, 2M); 0 means none.
    #[arg(long, value_name = "RATE", default_value = "0", value_parser = crate::ratelimit::parse_rate)]
    max_down: u64,
    /// Upload speed limit for the whole run, as for --max-down.
    #[arg(long, value_name = "RATE", default_value = "0", value_parser = crate::ratelimit::parse_rate)]
    max_up: u64,
    /// Full-screen view with a speed graph, piece map and peer table
    /// (needs a terminal; otherwise the status line is printed).
    #[arg(long)]
    dashboard: bool,
}

/// Runs the program and returns the process exit code.
pub fn run() -> Result<u8> {
    let cli = Cli::parse();
    init_logging(cli.verbose);
    match cli.command {
        Command::Info { torrent } => {
            info(&torrent)?;
            Ok(0)
        }
        Command::Download(args) => {
            let rt = tokio::runtime::Runtime::new().context("cannot start the async runtime")?;
            rt.block_on(download(args))
        }
        Command::Simulate(args) => {
            sim::run_cli(&args)?;
            Ok(0)
        }
    }
}

fn init_logging(verbose: u8) {
    let default = match verbose {
        0 => "warn",
        1 => "oxtorrent=info,warn",
        2 => "oxtorrent=debug,info",
        _ => "oxtorrent=trace,debug",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn load(path: &PathBuf) -> Result<Torrent> {
    let bytes = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    Torrent::from_bytes(&bytes)
        .with_context(|| format!("{} is not a valid torrent file", path.display()))
}

fn info(path: &PathBuf) -> Result<()> {
    let t = load(path)?;
    println!("Name:          {}", t.name);
    println!(
        "Total size:    {} ({} bytes)",
        human_bytes(t.total_length as f64),
        t.total_length
    );
    println!(
        "Piece length:  {} ({} bytes)",
        human_bytes(t.piece_length as f64),
        t.piece_length
    );
    println!("Pieces:        {}", t.piece_count());
    let trackers = t.trackers();
    if trackers.is_empty() {
        println!("Tracker:       (none)");
    }
    for (i, url) in trackers.iter().enumerate() {
        println!(
            "{}{url}",
            if i == 0 {
                "Tracker:       "
            } else {
                "               "
            }
        );
    }
    println!("Info hash:     {}", t.info_hash_hex());
    println!("Files:");
    for f in &t.files {
        println!(
            "  {:>12}  {}",
            human_bytes(f.length as f64),
            f.path.display()
        );
    }
    Ok(())
}

/// A torrent named on the command line, before its metadata is known.
enum Source {
    File(Torrent),
    Magnet(Magnet),
}

impl Source {
    fn info_hash(&self) -> [u8; 20] {
        match self {
            Source::File(t) => t.info_hash,
            Source::Magnet(m) => m.info_hash,
        }
    }

    fn label(&self) -> String {
        match self {
            Source::File(t) => t.name.clone(),
            Source::Magnet(m) => m
                .name
                .clone()
                .unwrap_or_else(|| crate::metainfo::hex(&m.info_hash)),
        }
    }

    fn private(&self) -> bool {
        matches!(self, Source::File(t) if t.private)
    }
}

async fn download(args: DownloadArgs) -> Result<u8> {
    let DownloadArgs {
        torrents,
        output,
        port,
        max_peers,
        seed,
        pipeline,
        peers,
        no_dht,
        max_down,
        max_up,
        dashboard,
    } = args;
    let mut sources = Vec::new();
    let mut seen = HashSet::new();
    for s in &torrents {
        let src = if s.starts_with("magnet:") {
            Source::Magnet(Magnet::parse(s).with_context(|| format!("invalid magnet link {s}"))?)
        } else {
            Source::File(load(&PathBuf::from(s))?)
        };
        if !seen.insert(src.info_hash()) {
            anyhow::bail!("{} is given more than once", src.label());
        }
        sources.push(src);
    }
    std::fs::create_dir_all(&output)
        .with_context(|| format!("cannot create output folder {}", output.display()))?;

    let listener = match TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await {
        Ok(l) => Some(l),
        Err(e) => {
            warn!("cannot listen on port {port} ({e}); incoming peers are disabled");
            None
        }
    };
    let real_port = listener
        .as_ref()
        .and_then(|l| l.local_addr().ok())
        .map_or(port, |a| a.port());

    let token = CancellationToken::new();
    let ctrl_c_token = token.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nShutting down...");
            ctrl_c_token.cancel();
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        }
    });

    // Every torrent is reached on the same port: the router reads each
    // incoming handshake and passes the connection to the right session.
    let router = listener.map(|l| Router::spawn(l, token.clone()));

    // The DHT listens on the same port number as the TCP listener, over UDP.
    // It joins the network in the background while the downloads start.
    let dht = if !no_dht && sources.iter().any(|s| !s.private()) {
        match Dht::start(
            DhtConfig {
                port: real_port,
                ..DhtConfig::default()
            },
            token.clone(),
        )
        .await
        {
            Ok(d) => Some(d),
            Err(e) => {
                warn!("cannot start the DHT ({e}); using trackers only");
                None
            }
        }
    } else {
        None
    };

    let mut base = Config::new(output, generate_peer_id());
    base.port = real_port;
    // --max-peers is the total for the run, shared evenly.
    base.max_peers = (max_peers / sources.len()).max(1);
    base.seed = seed;
    base.pipeline = pipeline.max(1);
    base.extra_peers = peers;
    base.dht = dht;
    base.limits = Limits::new(max_down, max_up);

    let mut receivers = Vec::new();
    let mut tasks = Vec::new();
    for src in sources {
        let label = src.label();
        let (tx, rx) = watch::channel(Progress {
            name: label.clone(),
            ..Progress::default()
        });
        receivers.push(rx);
        let inbound = router.as_ref().map_or(Inbound::None, |r| {
            Inbound::Routed(r.register(src.info_hash()))
        });
        let task = tokio::spawn(run_one(src, base.clone(), inbound, tx, token.clone()));
        tasks.push((label, task));
    }

    let printer = if dashboard && std::io::stdout().is_terminal() {
        tokio::spawn(crate::dashboard::run(receivers, token.clone()))
    } else {
        tokio::spawn(print_progress(receivers, token.clone()))
    };
    let mut results = Vec::new();
    for (label, task) in tasks {
        results.push((label, task.await));
    }
    token.cancel();
    let _ = printer.await;

    let many = results.len() > 1;
    let (mut failed, mut incomplete) = (false, false);
    for (label, r) in results {
        let prefix = if many {
            format!("{label}: ")
        } else {
            String::new()
        };
        match r {
            Ok(Ok(Some(o))) if o.complete => eprintln!(
                "{prefix}Done: all pieces verified ({} downloaded, {} uploaded).",
                human_bytes(o.stats.downloaded as f64),
                human_bytes(o.stats.uploaded as f64)
            ),
            Ok(Ok(Some(_))) => {
                incomplete = true;
                eprintln!(
                    "error: {prefix}download interrupted before completion; \
                     run the same command again to resume"
                );
            }
            Ok(Ok(None)) => {
                incomplete = true;
                eprintln!("error: {prefix}interrupted before the metadata arrived");
            }
            Ok(Err(e)) => {
                failed = true;
                eprintln!("error: {prefix}{e:#}");
            }
            Err(e) => {
                failed = true;
                eprintln!("error: {prefix}{e}");
            }
        }
    }
    Ok(if failed {
        1
    } else if incomplete {
        2
    } else {
        0
    })
}

/// Fetches a magnet link's metadata if needed, then runs the download.
/// Returns `None` if cancelled before the metadata arrived.
async fn run_one(
    src: Source,
    mut config: Config,
    inbound: Inbound,
    progress_tx: watch::Sender<Progress>,
    token: CancellationToken,
) -> Result<Option<Outcome>> {
    let torrent = match src {
        Source::File(t) => t,
        Source::Magnet(m) => {
            let mut fetch = FetchConfig::new(config.peer_id, config.port);
            fetch.dht = config.dht.clone();
            let dir = config.output_dir.clone();
            match torrent_from_magnet(&m, &fetch, &mut config.extra_peers, &dir, &token).await? {
                Some(t) => t,
                None => return Ok(None),
            }
        }
    };
    eprintln!(
        "Downloading {} ({}, {} pieces)",
        torrent.name,
        human_bytes(torrent.total_length as f64),
        torrent.piece_count()
    );
    let outcome = session::run_download(torrent, config, inbound, progress_tx, token).await?;
    Ok(Some(outcome))
}

/// Fetches the info dictionary for a magnet link and saves it as a
/// `.torrent` file in the output folder, so a later run can resume from the
/// file. Returns `None` if cancelled. Peers seen while fetching are added to
/// `peers`.
async fn torrent_from_magnet(
    m: &Magnet,
    fetch: &FetchConfig,
    peers: &mut Vec<SocketAddr>,
    output: &std::path::Path,
    token: &CancellationToken,
) -> Result<Option<Torrent>> {
    let label = m
        .name
        .clone()
        .unwrap_or_else(|| crate::metainfo::hex(&m.info_hash));
    eprintln!("Fetching metadata for {label} from peers...");
    let fetched = match magnet::fetch_metadata(m, fetch, peers, token).await {
        Ok(f) => f,
        Err(MagnetError::Cancelled) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let torrent = Torrent::from_info(&fetched.info, &m.trackers)
        .context("peers sent metadata that is not a valid info dictionary")?;
    for p in fetched.peers {
        if !peers.contains(&p) {
            peers.push(p);
        }
    }
    let file = output.join(format!("{}.torrent", torrent.name));
    match std::fs::write(&file, torrent.to_file_bytes()) {
        Ok(()) => eprintln!("Saved the torrent file to {}", file.display()),
        Err(e) => warn!("cannot save {}: {e}", file.display()),
    }
    Ok(Some(torrent))
}

/// Prints progress once a second until `token` is cancelled. With one
/// torrent that is its status line. With several, a terminal gets one
/// combined line, and other outputs (a log file, a pipe) get a line per
/// torrent, prefixed with its name.
async fn print_progress(rxs: Vec<watch::Receiver<Progress>>, token: CancellationToken) {
    let tty = std::io::stdout().is_terminal();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut printed = false;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = token.cancelled() => break,
        }
        let snaps: Vec<Progress> = rxs.iter().map(|r| r.borrow().clone()).collect();
        let lines = progress_lines(&snaps, tty);
        if lines.is_empty() {
            continue;
        }
        let mut out = std::io::stdout().lock();
        for line in lines {
            if tty {
                let _ = write!(out, "\r{line:<100}");
            } else {
                let _ = writeln!(out, "{line}");
            }
        }
        let _ = out.flush();
        printed = true;
    }
    if printed && tty {
        println!();
    }
}

pub fn progress_lines(snaps: &[Progress], tty: bool) -> Vec<String> {
    let started: Vec<&Progress> = snaps.iter().filter(|p| p.total > 0).collect();
    if started.is_empty() {
        return Vec::new();
    }
    if snaps.len() == 1 {
        return vec![status_line(started[0])];
    }
    if tty {
        let done = snaps.iter().filter(|p| p.complete).count();
        let all = Progress::combine(snaps);
        return vec![format!(
            "{done}/{} done  {}",
            snaps.len(),
            status_line(&all)
        )];
    }
    started
        .iter()
        .map(|p| format!("{}: {}", p.name, status_line(p)))
        .collect()
}

pub fn status_line(p: &Progress) -> String {
    let pct = if p.total > 0 {
        p.verified_bytes as f64 * 100.0 / p.total as f64
    } else {
        0.0
    };
    let left = p.total.saturating_sub(p.verified_bytes);
    let eta = if p.complete {
        "done".to_string()
    } else if p.download_rate > 1.0 {
        format_duration(left as f64 / p.download_rate)
    } else {
        "--".to_string()
    };
    format!(
        "{pct:5.1}%  {}/{} pieces  down {}/s  up {}/s  peers {}  eta {eta}",
        p.pieces_done,
        p.piece_count,
        human_bytes(p.download_rate),
        human_bytes(p.upload_rate),
        p.peers,
    )
}

pub fn human_bytes(n: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{v:.0} {}", UNITS[u])
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

pub fn format_duration(secs: f64) -> String {
    let s = secs.round() as u64;
    if s >= 3600 {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats() {
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(1536.0), "1.5 KiB");
        assert_eq!(format_duration(3725.0), "1h02m");
        assert_eq!(format_duration(65.0), "1m05s");
    }

    #[test]
    fn progress_lines_for_one_and_several_torrents() {
        let p = |name: &str, done: u64, complete: bool| Progress {
            name: name.to_string(),
            total: 100,
            verified_bytes: done,
            piece_count: 10,
            pieces_done: (done / 10) as usize,
            peers: 2,
            complete,
            ..Progress::default()
        };
        let not_started = Progress::default();
        assert!(progress_lines(std::slice::from_ref(&not_started), true).is_empty());

        let one = progress_lines(&[p("a", 50, false)], true);
        assert_eq!(one.len(), 1);
        assert!(one[0].starts_with(" 50.0%"), "{}", one[0]);

        let both = [p("a", 50, false), p("b", 100, true)];
        let tty = progress_lines(&both, true);
        assert_eq!(tty.len(), 1);
        assert!(tty[0].starts_with("1/2 done   75.0%"), "{}", tty[0]);
        assert!(tty[0].contains("peers 4"));

        let log = progress_lines(&both, false);
        assert_eq!(log.len(), 2);
        assert!(log[0].starts_with("a:  50.0%"), "{}", log[0]);
        assert!(log[1].starts_with("b: 100.0%"), "{}", log[1]);

        // A magnet still fetching its metadata is left out of the log lines.
        let log = progress_lines(&[p("a", 50, false), not_started], false);
        assert_eq!(log.len(), 1);
    }
}
