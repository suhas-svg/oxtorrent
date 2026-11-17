//! One listening socket shared by several downloads.
//!
//! Every torrent in a run is reached on the same port, so the router reads
//! each incoming handshake to learn the info hash, then hands the
//! connection, with the handshake already read, to that torrent's session.
//! Connections for torrents not in the run are closed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::peer::handshake::{self, Handshake};

/// Time an incoming peer has to send its handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Incoming connections whose handshake is being read at once; more wait
/// in the kernel's accept queue.
const MAX_PENDING: usize = 64;
/// Connections queued for one session before new ones are dropped.
const ROUTE_QUEUE: usize = 16;

/// An incoming connection whose handshake has been read.
pub struct RoutedPeer {
    pub stream: TcpStream,
    pub addr: SocketAddr,
    pub handshake: Handshake,
}

type Routes = Arc<Mutex<HashMap<[u8; 20], mpsc::Sender<RoutedPeer>>>>;

#[derive(Clone)]
pub struct Router {
    routes: Routes,
}

impl Router {
    /// Starts accepting on `listener` until `token` is cancelled.
    pub fn spawn(listener: TcpListener, token: CancellationToken) -> Router {
        let routes: Routes = Arc::default();
        tokio::spawn(accept_loop(listener, routes.clone(), token));
        Router { routes }
    }

    /// Connections for `info_hash` from now on arrive on the returned
    /// receiver. Registering the same hash again replaces the old route.
    pub fn register(&self, info_hash: [u8; 20]) -> mpsc::Receiver<RoutedPeer> {
        let (tx, rx) = mpsc::channel(ROUTE_QUEUE);
        if let Ok(mut r) = self.routes.lock() {
            r.insert(info_hash, tx);
        }
        rx
    }
}

async fn accept_loop(listener: TcpListener, routes: Routes, token: CancellationToken) {
    let pending = Arc::new(Semaphore::new(MAX_PENDING));
    loop {
        let permit = tokio::select! {
            _ = token.cancelled() => break,
            p = pending.clone().acquire_owned() => match p { Ok(p) => p, Err(_) => break },
        };
        let (mut stream, addr) = tokio::select! {
            _ = token.cancelled() => break,
            r = listener.accept() => match r {
                Ok(x) => x,
                Err(e) => {
                    warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let routes = routes.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let hs = match timeout(HANDSHAKE_TIMEOUT, handshake::read(&mut stream)).await {
                Ok(Ok(hs)) => hs,
                Ok(Err(e)) => return debug!(peer = %addr, "bad incoming handshake: {e}"),
                Err(_) => return debug!(peer = %addr, "incoming handshake timed out"),
            };
            let route = routes
                .lock()
                .ok()
                .and_then(|r| r.get(&hs.info_hash).cloned());
            let Some(tx) = route else {
                return debug!(peer = %addr, "incoming peer wants a torrent we do not have");
            };
            let peer = RoutedPeer {
                stream,
                addr,
                handshake: hs,
            };
            if tx.try_send(peer).is_err() {
                debug!(peer = %addr, "session busy or gone; dropping incoming peer");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn routes_by_info_hash_and_closes_unknown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let token = CancellationToken::new();
        let router = Router::spawn(listener, token.clone());
        let mut a = router.register([1; 20]);
        let mut b = router.register([2; 20]);

        // Keep the streams open until routed.
        let mut open = Vec::new();
        for hash in [[2u8; 20], [1; 20]] {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(&Handshake::new(hash, [9; 20]).to_bytes())
                .await
                .unwrap();
            open.push(s);
        }
        let got_b = timeout(Duration::from_secs(5), b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got_b.handshake.info_hash, [2; 20]);
        let got_a = timeout(Duration::from_secs(5), a.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got_a.handshake.info_hash, [1; 20]);
        assert_eq!(got_a.handshake.peer_id, [9; 20]);

        // An unknown torrent: the router closes the connection.
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(&Handshake::new([3; 20], [9; 20]).to_bytes())
            .await
            .unwrap();
        let mut buf = [0u8; 1];
        let n = timeout(Duration::from_secs(5), s.read(&mut buf))
            .await
            .unwrap()
            .unwrap_or(0);
        assert_eq!(n, 0);
        token.cancel();
    }
}
