//! The built-in swarm simulator.
//!
//! It runs the client's real piece-selection ([`crate::picker`]) and choking
//! ([`crate::choke`]) strategies against hundreds of simulated peers in one
//! process and writes one CSV row per peer.
//!
//! Limits of the model: no packet loss, no connection setup time, no TCP
//! ramp-up, and peers transfer whole pieces rather than 16 KiB blocks.
//! Results describe this model, not the real internet.

pub mod engine;
pub mod scenario;

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};

pub use engine::{run, PeerResult, RunResult};
pub use scenario::{ChokeStrategyKind, Group, PieceStrategyKind, Scenario};

#[derive(clap::Args, Debug, Clone)]
pub struct SimulateArgs {
    /// Scenario file (TOML).
    #[arg(long)]
    pub scenario: PathBuf,
    /// Seed of the first run; run k uses seed + k.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Number of runs with consecutive seeds.
    #[arg(long, default_value_t = 1)]
    pub runs: u64,
    /// CSV output: one row per peer per run.
    #[arg(long)]
    pub out: PathBuf,
    /// Override the scenario's piece strategy (rarest_first, random, sequential).
    #[arg(long)]
    pub piece_strategy: Option<PieceStrategyKind>,
    /// Override the scenario's choke strategy (tit_for_tat, random, no_reward).
    #[arg(long)]
    pub choke_strategy: Option<ChokeStrategyKind>,
    /// Override the number of upload slots.
    #[arg(long)]
    pub slots: Option<usize>,
    /// Also write the lowest piece availability over time to this CSV.
    #[arg(long)]
    pub availability_out: Option<PathBuf>,
}

pub fn run_cli(args: &SimulateArgs) -> Result<()> {
    let text = fs::read_to_string(&args.scenario)
        .with_context(|| format!("cannot read {}", args.scenario.display()))?;
    let mut sc = Scenario::from_toml(&text)
        .map_err(|e| anyhow::anyhow!("{}: {e}", args.scenario.display()))?;
    if let Some(p) = args.piece_strategy {
        sc.piece_strategy = p;
    }
    if let Some(c) = args.choke_strategy {
        sc.choke_strategy = c;
    }
    if let Some(s) = args.slots {
        sc.upload_slots = s.max(1);
    }

    let runs: Vec<RunResult> = (0..args.runs.max(1))
        .map(|k| run(&sc, args.seed + k))
        .collect();

    let mut out = Vec::new();
    write_csv(&runs, &mut out)?;
    fs::write(&args.out, out).with_context(|| format!("cannot write {}", args.out.display()))?;
    if let Some(path) = &args.availability_out {
        let mut w = csv::Writer::from_path(path)?;
        w.write_record(["seed", "time", "min_availability"])?;
        for r in &runs {
            for (t, a) in &r.min_availability {
                w.write_record([r.seed.to_string(), format!("{t:.1}"), a.to_string()])?;
            }
        }
        w.flush()?;
    }

    let summary = summarize_runs(&runs);
    let mut err = std::io::stderr().lock();
    writeln!(
        err,
        "scenario {} | piece {:?} | choke {:?} | slots {} | {} run(s)",
        sc.name,
        sc.piece_strategy,
        sc.choke_strategy,
        sc.upload_slots,
        runs.len()
    )?;
    writeln!(err, "{summary}")?;
    writeln!(err, "wrote {}", args.out.display())?;
    Ok(())
}

pub fn write_csv(runs: &[RunResult], out: &mut Vec<u8>) -> Result<()> {
    let mut w = csv::Writer::from_writer(out);
    w.write_record([
        "seed",
        "peer",
        "group",
        "is_seed",
        "join_time",
        "finish_time",
        "download_time",
        "left_time",
        "bytes_uploaded",
        "bytes_downloaded",
    ])?;
    let opt = |v: Option<f64>| v.map(|x| format!("{x:.3}")).unwrap_or_default();
    for r in runs {
        for p in &r.peers {
            w.write_record([
                r.seed.to_string(),
                p.peer.to_string(),
                p.group.clone(),
                p.is_seed.to_string(),
                format!("{:.3}", p.join_time),
                opt(p.finish_time),
                opt(p.download_time()),
                opt(p.left_time),
                p.bytes_uploaded.to_string(),
                p.bytes_downloaded.to_string(),
            ])?;
        }
    }
    w.flush()?;
    Ok(())
}

/// Per-run figures over the downloaders (non-seeds).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSummary {
    pub finished_share: f64,
    pub mean_time: f64,
    /// Mean download time of the slowest 5% of finishers.
    pub slowest5_mean: f64,
}

pub fn summarize(r: &RunResult) -> RunSummary {
    let leechers: Vec<&PeerResult> = r.peers.iter().filter(|p| !p.is_seed).collect();
    let mut times: Vec<f64> = leechers.iter().filter_map(|p| p.download_time()).collect();
    times.sort_by(f64::total_cmp);
    let finished_share = if leechers.is_empty() {
        0.0
    } else {
        times.len() as f64 / leechers.len() as f64
    };
    let mean = |v: &[f64]| {
        if v.is_empty() {
            f64::NAN
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };
    let k = times.len().div_ceil(20);
    RunSummary {
        finished_share,
        mean_time: mean(&times),
        slowest5_mean: mean(&times[times.len() - k..]),
    }
}

pub struct Summary(Vec<RunSummary>);

pub fn summarize_runs(runs: &[RunResult]) -> Summary {
    Summary(runs.iter().map(summarize).collect())
}

fn mean_sd(v: impl Iterator<Item = f64>) -> (f64, f64) {
    let v: Vec<f64> = v.filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return (f64::NAN, f64::NAN);
    }
    let m = v.iter().sum::<f64>() / v.len() as f64;
    let var = v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / v.len() as f64;
    (m, var.sqrt())
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (fs, fsd) = mean_sd(self.0.iter().map(|s| s.finished_share * 100.0));
        let (mt, msd) = mean_sd(self.0.iter().map(|s| s.mean_time));
        let (st, ssd) = mean_sd(self.0.iter().map(|s| s.slowest5_mean));
        write!(
            f,
            "finished {fs:.1}% (sd {fsd:.1}) | mean download {mt:.1}s (sd {msd:.1}) | slowest 5% {st:.1}s (sd {ssd:.1})"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: f64 = 1024.0 * 1024.0;

    fn group(name: &str, count: usize, up: f64, seed: bool) -> Group {
        Group {
            name: name.into(),
            count,
            upload_speed: up,
            download_speed: 100.0 * MIB,
            seed,
            join_start: 0.0,
            join_end: 0.0,
            leave_after_complete: None,
            leave_after_uploaded_copies: None,
        }
    }

    fn scenario(groups: Vec<Group>) -> Scenario {
        Scenario {
            name: "test".into(),
            file_size: 10 * 1024 * 1024,
            piece_size: 256 * 1024,
            piece_strategy: PieceStrategyKind::RarestFirst,
            choke_strategy: ChokeStrategyKind::TitForTat,
            upload_slots: 4,
            max_neighbors: 40,
            choke_interval_secs: 10.0,
            max_time_secs: 36_000.0,
            sample_interval_secs: 60.0,
            groups,
        }
    }

    #[test]
    fn one_seeder_one_downloader_matches_formula() {
        let sc = scenario(vec![
            group("seed", 1, MIB, true),
            group("dl", 1, MIB, false),
        ]);
        let r = run(&sc, 1);
        let t = r.peers[1].download_time().unwrap();
        let expected = sc.file_size as f64 / MIB;
        assert!(
            (t - expected).abs() < 1e-3,
            "took {t}s, expected {expected}s"
        );
    }

    #[test]
    fn bytes_are_conserved() {
        let mut dl = group("dl", 60, 256.0 * 1024.0, false);
        dl.join_end = 120.0;
        dl.leave_after_complete = Some(30.0);
        let mut seed = group("seed", 2, MIB, true);
        seed.leave_after_uploaded_copies = Some(3.0);
        let sc = scenario(vec![seed, dl]);
        let r = run(&sc, 9);
        let up: u64 = r.peers.iter().map(|p| p.bytes_uploaded).sum();
        let down: u64 = r.peers.iter().map(|p| p.bytes_downloaded).sum();
        assert_eq!(up, down);
        assert!(down > 0);
    }

    #[test]
    fn same_seed_same_csv() {
        let mut dl = group("dl", 40, 128.0 * 1024.0, false);
        dl.join_end = 60.0;
        let sc = scenario(vec![group("seed", 1, MIB, true), dl]);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        write_csv(&[run(&sc, 42)], &mut a).unwrap();
        write_csv(&[run(&sc, 42)], &mut b).unwrap();
        assert_eq!(a, b);
        let mut c = Vec::new();
        write_csv(&[run(&sc, 43)], &mut c).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn everyone_finishes_in_a_healthy_swarm() {
        let mut dl = group("dl", 50, 256.0 * 1024.0, false);
        dl.join_end = 30.0;
        let sc = scenario(vec![group("seed", 1, MIB, true), dl]);
        let r = run(&sc, 5);
        for p in r
            .peers
            .iter()
            .filter(|p| !p.is_seed && p.finish_time.is_none())
        {
            eprintln!("unfinished: {p:?} end {}", r.end_time);
        }
        let s = summarize(&r);
        assert_eq!(s.finished_share, 1.0);
    }

    #[test]
    fn scenario_files_parse() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
        let mut n = 0;
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "toml") {
                let text = std::fs::read_to_string(&path).unwrap();
                Scenario::from_toml(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                n += 1;
            }
        }
        assert_eq!(n, 5);
    }
}
