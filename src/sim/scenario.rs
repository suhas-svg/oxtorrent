//! Scenario files: what swarm to simulate.

use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PieceStrategyKind {
    RarestFirst,
    Random,
    Sequential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChokeStrategyKind {
    TitForTat,
    Random,
    NoReward,
}

impl std::str::FromStr for PieceStrategyKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "rarest_first" => Ok(Self::RarestFirst),
            "random" => Ok(Self::Random),
            "sequential" => Ok(Self::Sequential),
            _ => Err(format!(
                "unknown piece strategy {s:?} (rarest_first, random, sequential)"
            )),
        }
    }
}

impl std::str::FromStr for ChokeStrategyKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "tit_for_tat" => Ok(Self::TitForTat),
            "random" => Ok(Self::Random),
            "no_reward" => Ok(Self::NoReward),
            _ => Err(format!(
                "unknown choke strategy {s:?} (tit_for_tat, random, no_reward)"
            )),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    /// Bytes.
    pub file_size: u64,
    /// Bytes.
    pub piece_size: u64,
    #[serde(default = "default_piece_strategy")]
    pub piece_strategy: PieceStrategyKind,
    #[serde(default = "default_choke_strategy")]
    pub choke_strategy: ChokeStrategyKind,
    /// Upload slots per peer, including the optimistic slot.
    #[serde(default = "default_slots")]
    pub upload_slots: usize,
    /// How many other peers each peer is connected to.
    #[serde(default = "default_neighbors")]
    pub max_neighbors: usize,
    #[serde(default = "default_choke_interval")]
    pub choke_interval_secs: f64,
    /// Simulated time after which the run stops.
    #[serde(default = "default_max_time")]
    pub max_time_secs: f64,
    /// Every this many seconds, record the lowest piece availability.
    #[serde(default = "default_sample")]
    pub sample_interval_secs: f64,
    pub groups: Vec<Group>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub name: String,
    pub count: usize,
    /// Bytes per second. Zero makes a free rider.
    pub upload_speed: f64,
    /// Bytes per second.
    pub download_speed: f64,
    /// Starts with the whole file.
    #[serde(default)]
    pub seed: bool,
    /// Join times are spread evenly at random between these (seconds).
    #[serde(default)]
    pub join_start: f64,
    #[serde(default)]
    pub join_end: f64,
    /// Leave this many seconds after finishing. Absent: never leave.
    #[serde(default)]
    pub leave_after_complete: Option<f64>,
    /// Leave once this many full copies have been uploaded.
    #[serde(default)]
    pub leave_after_uploaded_copies: Option<f64>,
}

fn default_piece_strategy() -> PieceStrategyKind {
    PieceStrategyKind::RarestFirst
}
fn default_choke_strategy() -> ChokeStrategyKind {
    ChokeStrategyKind::TitForTat
}
fn default_slots() -> usize {
    4
}
fn default_neighbors() -> usize {
    40
}
fn default_choke_interval() -> f64 {
    10.0
}
fn default_max_time() -> f64 {
    24.0 * 3600.0
}
fn default_sample() -> f64 {
    60.0
}

impl Scenario {
    pub fn from_toml(text: &str) -> Result<Scenario, String> {
        let s: Scenario = toml::from_str(text).map_err(|e| e.to_string())?;
        s.validate()?;
        Ok(s)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.file_size == 0 || self.piece_size == 0 {
            return Err("file_size and piece_size must be positive".into());
        }
        if self.file_size.div_ceil(self.piece_size) > 1_000_000 {
            return Err("too many pieces (over 1,000,000)".into());
        }
        if self.upload_slots == 0 {
            return Err("upload_slots must be at least 1".into());
        }
        if self.choke_interval_secs <= 0.0 || self.sample_interval_secs <= 0.0 {
            return Err("intervals must be positive".into());
        }
        for g in &self.groups {
            if g.upload_speed < 0.0 || g.download_speed <= 0.0 {
                return Err(format!("group {}: speeds must be positive", g.name));
            }
            if g.join_end < g.join_start {
                return Err(format!("group {}: join_end is before join_start", g.name));
            }
        }
        if !self.groups.iter().any(|g| g.seed && g.count > 0) {
            return Err("at least one group must be a seeder".into());
        }
        Ok(())
    }
}
