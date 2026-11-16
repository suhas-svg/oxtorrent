//! Argument parsing and progress output.

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

use crate::metainfo::Torrent;
use crate::peer::generate_peer_id;
use crate::session::{self, Config, Progress};
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
    /// Download a torrent.
    Download {
        torrent: PathBuf,
        /// Where to save the files.
        #[arg(long, default_value = ".")]
        output: PathBuf,
        /// TCP port for incoming peers.
        #[arg(long, default_value_t = 6881)]
        port: u16,
        /// Maximum simultaneous connections.
        #[arg(long, default_value_t = 50)]
        max_peers: usize,
        /// Keep uploading after the download completes (until Ctrl-C).
        #[arg(long)]
        seed: bool,
        /// Outstanding block requests per peer.
        #[arg(long, default_value_t = 5)]
        pipeline: usize,
        /// Extra peer to connect to (ip:port); may be repeated.
        #[arg(long = "peer")]
        peers: Vec<SocketAddr>,
    },
    /// Run the swarm simulator on a scenario file.
    Simulate(sim::SimulateArgs),
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
        Command::Download {
            torrent,
            output,
            port,
            max_peers,
            seed,
            pipeline,
            peers,
        } => {
            let rt = tokio::runtime::Runtime::new().context("cannot start the async runtime")?;
            rt.block_on(download(
                torrent, output, port, max_peers, seed, pipeline, peers,
            ))
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

async fn download(
    path: PathBuf,
    output: PathBuf,
    port: u16,
    max_peers: usize,
    seed: bool,
    pipeline: usize,
    peers: Vec<SocketAddr>,
) -> Result<u8> {
    let torrent = load(&path)?;
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

    let mut config = Config::new(output, generate_peer_id());
    config.port = real_port;
    config.max_peers = max_peers.max(1);
    config.seed = seed;
    config.pipeline = pipeline.max(1);
    config.extra_peers = peers;

    eprintln!(
        "Downloading {} ({}, {} pieces)",
        torrent.name,
        human_bytes(torrent.total_length as f64),
        torrent.piece_count()
    );

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

    let (progress_tx, progress_rx) = watch::channel(Progress::default());
    let printer = tokio::spawn(print_progress(progress_rx, token.clone()));
    let outcome =
        session::run_download(torrent, config, listener, progress_tx, token.clone()).await;
    token.cancel();
    let _ = printer.await;

    let outcome = outcome?;
    if outcome.complete {
        eprintln!(
            "Done: all pieces verified ({} downloaded, {} uploaded).",
            human_bytes(outcome.stats.downloaded as f64),
            human_bytes(outcome.stats.uploaded as f64)
        );
        Ok(0)
    } else {
        eprintln!(
            "error: download interrupted before completion; run the same command again to resume"
        );
        Ok(2)
    }
}

async fn print_progress(mut rx: watch::Receiver<Progress>, token: CancellationToken) {
    let tty = std::io::stdout().is_terminal();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut printed = false;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = token.cancelled() => break,
            r = rx.changed() => if r.is_err() { break } else { continue },
        }
        let p = rx.borrow().clone();
        if p.total == 0 {
            continue;
        }
        let line = status_line(&p);
        let mut out = std::io::stdout().lock();
        if tty {
            let _ = write!(out, "\r{line:<100}");
        } else {
            let _ = writeln!(out, "{line}");
        }
        let _ = out.flush();
        printed = true;
    }
    if printed && tty {
        println!();
    }
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

fn format_duration(secs: f64) -> String {
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
}
