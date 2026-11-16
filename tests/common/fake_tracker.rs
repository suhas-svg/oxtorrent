//! A tiny HTTP tracker for tests. It returns a fixed list of peers plus
//! every client that has announced to it (except the one asking).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct FakeTracker {
    pub url: String,
    pub announced: Arc<Mutex<Vec<SocketAddr>>>,
    pub announces: Arc<Mutex<Vec<String>>>,
}

pub async fn spawn(fixed: Vec<SocketAddr>) -> FakeTracker {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let announced: Arc<Mutex<Vec<SocketAddr>>> = Arc::default();
    let announces: Arc<Mutex<Vec<String>>> = Arc::default();
    let (reg, log) = (announced.clone(), announces.clone());
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                break;
            };
            let (reg, log, fixed) = (reg.clone(), log.clone(), fixed.clone());
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 8192 {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&buf).to_string();
                let line = text.lines().next().unwrap_or("").to_string();
                let query = line
                    .split(' ')
                    .nth(1)
                    .unwrap_or("")
                    .split('?')
                    .nth(1)
                    .unwrap_or("")
                    .to_string();
                let param = |k: &str| {
                    query
                        .split('&')
                        .find_map(|kv| kv.strip_prefix(&format!("{k}=")))
                        .map(str::to_string)
                };
                let port: u16 = param("port").and_then(|p| p.parse().ok()).unwrap_or(0);
                let me = SocketAddr::from(([127, 0, 0, 1], port));
                log.lock().unwrap().push(param("event").unwrap_or_default());
                {
                    let mut r = reg.lock().unwrap();
                    if param("event").as_deref() == Some("stopped") {
                        r.retain(|a| *a != me);
                    } else if !r.contains(&me) {
                        r.push(me);
                    }
                }
                let mut peers = Vec::new();
                for a in fixed.iter().chain(reg.lock().unwrap().iter()) {
                    if *a == me {
                        continue;
                    }
                    if let SocketAddr::V4(v4) = a {
                        peers.extend_from_slice(&v4.ip().octets());
                        peers.extend_from_slice(&v4.port().to_be_bytes());
                    }
                }
                let mut body = format!("d8:intervali1800e5:peers{}:", peers.len()).into_bytes();
                body.extend_from_slice(&peers);
                body.push(b'e');
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(&body).await;
                let _ = s.shutdown().await;
            });
        }
    });
    FakeTracker {
        url: format!("http://{addr}/announce"),
        announced,
        announces,
    }
}
