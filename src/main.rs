#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    match oxtorrent::cli::run() {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(1)
        }
    }
}
