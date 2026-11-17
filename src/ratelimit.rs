//! Speed limits: a token bucket shared by every peer connection in a run.
//!
//! A connection calls [`RateLimiter::reserve`] with the size of each block
//! it receives or is about to send. The bucket is charged at once, even
//! into debt, and the caller is told how long to wait until the debt is
//! paid off. Waiting connections queue up behind each other, so the
//! combined rate stays at the limit and each connection gets a turn
//! roughly once per block.
//!
//! Downloads are throttled by pausing reads from the socket, which slows
//! the sender through TCP flow control; uploads by holding blocks back.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Smallest burst the bucket allows, so a limit below one block per
/// second still lets whole blocks through.
const MIN_BURST: f64 = 32.0 * 1024.0;

/// The limits for a run; `None` is unlimited. Cloning shares the buckets,
/// so one pair covers every torrent and peer.
#[derive(Debug, Clone, Default)]
pub struct Limits {
    pub download: Option<Arc<RateLimiter>>,
    pub upload: Option<Arc<RateLimiter>>,
}

impl Limits {
    /// Limits in bytes per second; 0 means none.
    pub fn new(download: u64, upload: u64) -> Self {
        let make = |r: u64| (r > 0).then(|| Arc::new(RateLimiter::new(r)));
        Limits {
            download: make(download),
            upload: make(upload),
        }
    }
}

#[derive(Debug)]
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    /// A limit of `bytes_per_sec`, with a burst of a quarter second of
    /// traffic (at least 32 KiB).
    pub fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        let burst = (rate / 4.0).max(MIN_BURST);
        RateLimiter {
            rate,
            burst,
            state: Mutex::new((burst, Instant::now())),
        }
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// Charges `bytes` and returns how long the caller should wait before
    /// moving them (zero when the bucket had enough).
    pub fn reserve(&self, bytes: usize) -> Duration {
        self.reserve_at(bytes, Instant::now())
    }

    fn reserve_at(&self, bytes: usize, now: Instant) -> Duration {
        let Ok(mut s) = self.state.lock() else {
            return Duration::ZERO;
        };
        let (tokens, last) = &mut *s;
        let elapsed = now.saturating_duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * self.rate).min(self.burst);
        *last = now.max(*last);
        *tokens -= bytes as f64;
        if *tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-*tokens / self.rate)
        }
    }
}

/// Parses a rate such as `500K`, `2M`, `1.5MiB` or `800000` (bytes per
/// second; K and M are 1024-based). `0` means no limit.
pub fn parse_rate(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let lower = t.to_ascii_lowercase();
    let lower = lower
        .strip_suffix("/s")
        .unwrap_or(&lower)
        .trim_end_matches("ib")
        .trim_end_matches('b');
    let (num, mult) = match lower.chars().last() {
        Some('k') => (&lower[..lower.len() - 1], 1024.0),
        Some('m') => (&lower[..lower.len() - 1], 1024.0 * 1024.0),
        Some('g') => (&lower[..lower.len() - 1], 1024.0 * 1024.0 * 1024.0),
        _ => (lower, 1.0),
    };
    let v: f64 = num
        .trim()
        .parse()
        .map_err(|_| format!("{t:?} is not a rate; use e.g. 500K or 2M (bytes per second)"))?;
    if !v.is_finite() || v < 0.0 {
        return Err(format!("{t:?} is not a rate"));
    }
    Ok((v * mult).round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_then_steady_rate() {
        let l = RateLimiter::new(64 * 1024);
        let t0 = Instant::now();
        // The 32 KiB burst passes at once.
        assert_eq!(l.reserve_at(16384, t0), Duration::ZERO);
        assert_eq!(l.reserve_at(16384, t0), Duration::ZERO);
        // Then each 16 KiB block costs a quarter second, and waits add up.
        let w1 = l.reserve_at(16384, t0);
        let w2 = l.reserve_at(16384, t0);
        assert!((w1.as_secs_f64() - 0.25).abs() < 1e-6, "{w1:?}");
        assert!((w2.as_secs_f64() - 0.5).abs() < 1e-6, "{w2:?}");
        // After the debt is paid, the bucket refills up to the burst only.
        let later = t0 + Duration::from_secs(60);
        assert_eq!(l.reserve_at(32768, later), Duration::ZERO);
        assert!(l.reserve_at(16384, later) > Duration::ZERO);
    }

    #[test]
    fn long_run_matches_the_rate() {
        // Simulate a reader that waits as told: 10 MiB at 1 MiB/s takes
        // about 10 s.
        let l = RateLimiter::new(1024 * 1024);
        let t0 = Instant::now();
        let mut now = t0;
        for _ in 0..640 {
            now += l.reserve_at(16384, now);
        }
        let secs = (now - t0).as_secs_f64();
        assert!((9.7..=10.0).contains(&secs), "{secs}");
    }

    #[test]
    fn parses_rates() {
        assert_eq!(parse_rate("800000"), Ok(800_000));
        assert_eq!(parse_rate("500K"), Ok(512_000));
        assert_eq!(parse_rate("500k"), Ok(512_000));
        assert_eq!(parse_rate("2M"), Ok(2 * 1024 * 1024));
        assert_eq!(parse_rate("1.5MiB"), Ok(1_572_864));
        assert_eq!(parse_rate("2MB/s"), Ok(2 * 1024 * 1024));
        assert_eq!(parse_rate("0"), Ok(0));
        assert!(parse_rate("fast").is_err());
        assert!(parse_rate("-5K").is_err());
    }
}
