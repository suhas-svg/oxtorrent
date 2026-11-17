//! The `--dashboard` view: a full-screen summary redrawn once a second.
//!
//! [`Dashboard::render`] turns a [`Progress`] snapshot into lines of text
//! and knows nothing about terminals, so it is tested directly. [`run`]
//! draws those lines on the terminal's alternate screen and restores the
//! normal screen when the download ends or is interrupted.

use std::collections::VecDeque;
use std::io::Write;
use std::time::Duration;

use crossterm::{cursor, execute, queue, style, terminal};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::cli::{format_duration, human_bytes};
use crate::picker::PieceStatus;
use crate::session::{PeerRow, Progress};

/// Seconds of download speed kept for the sparkline.
const HISTORY: usize = 60;
const SPARKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

#[derive(Debug, Default)]
pub struct Dashboard {
    history: VecDeque<f64>,
}

impl Dashboard {
    /// Records one second's download speed.
    pub fn push(&mut self, p: &Progress) {
        self.history.push_back(p.download_rate);
        while self.history.len() > HISTORY {
            self.history.pop_front();
        }
    }

    /// Lines to draw, each at most `width` characters, at most `height`
    /// lines (the peer table is cut to fit).
    pub fn render(&self, name: &str, p: &Progress, width: usize, height: usize) -> Vec<String> {
        let width = width.max(40);
        let mut out = self.header(name, p, width);

        // Piece map: two rows, each cell covering a run of pieces.
        let cells = width.saturating_sub(8) * 2;
        let map = piece_map(&p.pieces, cells);
        let half = map.chars().count().div_ceil(2);
        out.push(format!(
            "pieces  {}",
            map.chars().take(half).collect::<String>()
        ));
        out.push(format!(
            "        {}",
            map.chars().skip(half).collect::<String>()
        ));
        out.push("        █ verified  ▓ mostly  ▒ started  · missing".to_string());
        out.push(String::new());

        let rows = p.peer_rows.iter().map(|r| (r, p.piece_count)).collect();
        peer_table(&mut out, rows, height);
        out.into_iter().map(|l| cut(&l, width)).collect()
    }

    /// The view for several torrents: combined totals, one row per
    /// torrent, then the peers of all of them.
    pub fn render_many(&self, items: &[Progress], width: usize, height: usize) -> Vec<String> {
        let width = width.max(40);
        let all = Progress::combine(items);
        let done = items.iter().filter(|p| p.complete).count();
        let title = format!("{} torrents, {done} done", items.len());
        let mut out = self.header(&title, &all, width);
        let name_width = width.saturating_sub(52).clamp(10, 40);
        out.push(format!(
            "{:<name_width$} {:>6} {:>11} {:>11} {:>5} {:>7}",
            "Torrent", "Done", "Down", "Up", "Peers", "ETA"
        ));
        for p in items {
            let pct = if p.total > 0 {
                format!("{:.1}%", p.verified_bytes as f64 * 100.0 / p.total as f64)
            } else {
                "-".to_string()
            };
            out.push(format!(
                "{:<name_width$} {:>6} {:>11} {:>11} {:>5} {:>7}",
                cut(&p.name, name_width),
                pct,
                format!("{}/s", human_bytes(p.download_rate)),
                format!("{}/s", human_bytes(p.upload_rate)),
                p.peers,
                eta(p),
            ));
        }
        out.push(String::new());
        let rows = items
            .iter()
            .flat_map(|p| p.peer_rows.iter().map(move |r| (r, p.piece_count)))
            .collect();
        peer_table(&mut out, rows, height);
        out.into_iter().map(|l| cut(&l, width)).collect()
    }

    /// Title, progress bar, rates and the speed sparkline.
    fn header(&self, name: &str, p: &Progress, width: usize) -> Vec<String> {
        let mut out = Vec::new();
        let pct = if p.total > 0 {
            p.verified_bytes as f64 * 100.0 / p.total as f64
        } else {
            0.0
        };
        let eta = eta(p);

        out.push(format!("Oxtorrent · {name}"));
        let bar_width = width.saturating_sub(42).clamp(10, 60);
        let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
        out.push(format!(
            "{pct:5.1}%  [{}{}]  {}/{} pieces  eta {eta}",
            "█".repeat(filled.min(bar_width)),
            "░".repeat(bar_width - filled.min(bar_width)),
            p.pieces_done,
            p.piece_count,
        ));
        out.push(format!(
            "down {}/s  up {}/s  peers {}  received {}  sent {}",
            human_bytes(p.download_rate),
            human_bytes(p.upload_rate),
            p.peers,
            human_bytes(p.downloaded as f64),
            human_bytes(p.uploaded as f64),
        ));
        let max = self.history.iter().copied().fold(0.0, f64::max);
        let spark: String = self
            .history
            .iter()
            .map(|&r| {
                if max <= 0.0 {
                    SPARKS[0]
                } else {
                    SPARKS[((r / max) * 7.0).round().clamp(0.0, 7.0) as usize]
                }
            })
            .collect();
        out.push(format!(
            "speed {spark}  (last {}s, peak {}/s)",
            self.history.len(),
            human_bytes(max)
        ));
        out.push(String::new());
        out
    }
}

fn eta(p: &Progress) -> String {
    let left = p.total.saturating_sub(p.verified_bytes);
    if p.complete {
        "done".to_string()
    } else if p.download_rate > 1.0 {
        format_duration(left as f64 / p.download_rate)
    } else {
        "--".to_string()
    }
}

/// The peer table, fastest first, cut to fit `height` lines in all. Each
/// row comes with its torrent's piece count, for the "Has" column.
fn peer_table(out: &mut Vec<String>, mut rows: Vec<(&PeerRow, usize)>, height: usize) {
    {
        out.push(format!(
            "{:<22} {:<18} {:>11} {:>11} {:<5} {:>4} {:>5}",
            "Peer", "Client", "Down", "Up", "Flags", "Reqs", "Has"
        ));
        rows.sort_by(|(a, _), (b, _)| {
            b.down_rate
                .total_cmp(&a.down_rate)
                .then(b.up_rate.total_cmp(&a.up_rate))
                .then(a.addr.cmp(&b.addr))
        });
        let room = height.saturating_sub(out.len() + 1);
        let shown = rows.len().min(room);
        for (r, piece_count) in &rows[..shown] {
            let has = (r.pieces * 100)
                .checked_div(*piece_count)
                .map_or_else(|| "-".to_string(), |pct| format!("{pct}%"));
            out.push(format!(
                "{:<22} {:<18} {:>11} {:>11} {:<5} {:>4} {:>5}",
                cut(&r.addr.to_string(), 22),
                cut(&r.client, 18),
                format!("{}/s", human_bytes(r.down_rate)),
                format!("{}/s", human_bytes(r.up_rate)),
                flags(r),
                r.requests,
                has,
            ));
        }
        if rows.len() > shown {
            out.push(format!("… and {} more", rows.len() - shown));
        }
    }
}

/// Two-letter state, as in other clients: `D` we are downloading (it
/// unchoked us and we want its pieces), `d` we want its pieces but are
/// choked; `U` we are uploading to it, `u` it wants ours but we choke it.
fn flags(r: &PeerRow) -> String {
    let d = match (r.am_interested, r.peer_choking) {
        (true, false) => 'D',
        (true, true) => 'd',
        _ => '.',
    };
    let u = match (r.peer_interested, r.am_choking) {
        (true, false) => 'U',
        (true, true) => 'u',
        _ => '.',
    };
    format!("{d}{u}")
}

/// One character per cell; each cell covers `pieces.len() / cells` pieces
/// (or one piece per cell when there are fewer pieces than cells).
pub fn piece_map(pieces: &[PieceStatus], cells: usize) -> String {
    if pieces.is_empty() || cells == 0 {
        return String::new();
    }
    let cells = cells.min(pieces.len());
    (0..cells)
        .map(|c| {
            let start = c * pieces.len() / cells;
            let end = ((c + 1) * pieces.len() / cells).max(start + 1);
            let run = &pieces[start..end];
            let done = run.iter().filter(|s| **s == PieceStatus::Verified).count();
            let started = run.contains(&PieceStatus::InProgress);
            if done == run.len() {
                '█'
            } else if done * 2 >= run.len() {
                '▓'
            } else if done > 0 || started {
                '▒'
            } else {
                '·'
            }
        })
        .collect()
}

/// Cuts `s` to at most `n` characters.
fn cut(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(n.saturating_sub(1)).collect();
        t.push('…');
        t
    }
}

/// Draws the dashboard once a second until `token` is cancelled, then
/// restores the screen.
pub async fn run(rxs: Vec<watch::Receiver<Progress>>, token: CancellationToken) {
    let mut out = std::io::stdout();
    if execute!(out, terminal::EnterAlternateScreen, cursor::Hide).is_err() {
        return;
    }
    let mut dash = Dashboard::default();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = token.cancelled() => break,
        }
        let snaps: Vec<Progress> = rxs.iter().map(|r| r.borrow().clone()).collect();
        let (w, h) = terminal::size().unwrap_or((100, 30));
        let lines = if let [p] = snaps.as_slice() {
            dash.push(p);
            dash.render(&p.name, p, w as usize, h as usize)
        } else {
            dash.push(&Progress::combine(&snaps));
            dash.render_many(&snaps, w as usize, h as usize)
        };
        let _ = queue!(
            out,
            cursor::MoveTo(0, 0),
            terminal::Clear(terminal::ClearType::All)
        );
        for (i, line) in lines.iter().enumerate() {
            let _ = queue!(out, cursor::MoveTo(0, i as u16), style::Print(line));
        }
        let _ = out.flush();
    }
    let _ = execute!(out, cursor::Show, terminal::LeaveAlternateScreen);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(port: u16, down: f64) -> PeerRow {
        PeerRow {
            addr: format!("10.0.0.1:{port}").parse().unwrap(),
            client: "qBittorrent 4630".to_string(),
            down_rate: down,
            up_rate: 0.0,
            peer_choking: false,
            am_interested: true,
            am_choking: true,
            peer_interested: true,
            requests: 12,
            pieces: 50,
        }
    }

    fn progress() -> Progress {
        let mut pieces = vec![PieceStatus::Verified; 50];
        pieces.extend(vec![PieceStatus::InProgress; 10]);
        pieces.extend(vec![PieceStatus::Missing; 40]);
        Progress {
            name: "test.iso".to_string(),
            total: 100 * 262_144,
            verified_bytes: 50 * 262_144,
            pieces_done: 50,
            piece_count: 100,
            downloaded: 13_500_000,
            uploaded: 0,
            download_rate: 2.0 * 1024.0 * 1024.0,
            upload_rate: 0.0,
            peers: 3,
            complete: false,
            peer_rows: vec![row(1, 100.0), row(2, 5000.0), row(3, 0.0)],
            pieces,
        }
    }

    #[test]
    fn renders_summary_map_and_sorted_peers() {
        let mut d = Dashboard::default();
        let p = progress();
        d.push(&p);
        let lines = d.render("test.iso", &p, 100, 40);
        assert_eq!(lines[0], "Oxtorrent · test.iso");
        assert!(lines[1].starts_with(" 50.0%  ["), "{}", lines[1]);
        assert!(lines[1].contains("50/100 pieces"));
        assert!(lines[2].contains("down 2.0 MiB/s"));
        assert!(lines[3].starts_with("speed █"));
        // The map shows the three regions in order.
        let map: String = lines[5][8..].chars().chain(lines[6][8..].chars()).collect();
        let first_missing = map.find('·').unwrap();
        assert!(
            map.starts_with('█') && map.contains('▒') && first_missing > map.find('▒').unwrap()
        );
        // Fastest peer first, with flags D (downloading) and u (choked by us).
        let table: Vec<&String> = lines.iter().filter(|l| l.starts_with("10.0.0.1")).collect();
        assert_eq!(table.len(), 3);
        assert!(table[0].starts_with("10.0.0.1:2 "));
        assert!(table[0].contains(" Du "));
        assert!(table[0].contains("50%"));
        assert!(lines.iter().all(|l| l.chars().count() <= 100));
    }

    #[test]
    fn peer_table_is_cut_to_the_screen() {
        let mut p = progress();
        p.peer_rows = (0..40).map(|i| row(i, i as f64)).collect();
        let lines = Dashboard::default().render("x", &p, 80, 20);
        assert!(lines.len() <= 20);
        assert!(lines.last().unwrap().starts_with("… and "));
        assert!(lines.iter().all(|l| l.chars().count() <= 80));
    }

    #[test]
    fn several_torrents_get_a_row_each_and_shared_peer_table() {
        let mut a = progress();
        a.name = "first.iso".to_string();
        let mut b = progress();
        b.name = "second.iso".to_string();
        b.complete = true;
        b.verified_bytes = b.total;
        b.peer_rows = vec![row(9, 9000.0)];
        let mut d = Dashboard::default();
        d.push(&Progress::combine(&[a.clone(), b.clone()]));
        let lines = d.render_many(&[a, b], 100, 40);
        assert_eq!(lines[0], "Oxtorrent · 2 torrents, 1 done");
        assert!(lines[1].starts_with(" 75.0%  ["), "{}", lines[1]);
        assert!(lines[2].contains("peers 6"));
        let first = lines.iter().find(|l| l.starts_with("first.iso")).unwrap();
        assert!(first.contains("50.0%"), "{first}");
        let second = lines.iter().find(|l| l.starts_with("second.iso")).unwrap();
        assert!(
            second.contains("100.0%") && second.contains("done"),
            "{second}"
        );
        // Peers of both torrents, fastest first.
        let table: Vec<&String> = lines.iter().filter(|l| l.starts_with("10.0.0.1")).collect();
        assert_eq!(table.len(), 4);
        assert!(table[0].starts_with("10.0.0.1:9 "));
        assert!(lines.iter().all(|l| l.chars().count() <= 100));
    }

    #[test]
    fn piece_map_cells() {
        use PieceStatus::*;
        assert_eq!(piece_map(&[Verified, Missing, InProgress], 10), "█·▒");
        assert_eq!(piece_map(&[Verified, Verified, Verified, Missing], 2), "█▓");
        assert_eq!(piece_map(&[Missing; 8], 2), "··");
        assert_eq!(piece_map(&[], 5), "");
    }
}
