use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{self, Duration};
use tokio_tungstenite::connect_async;
use tungstenite::{client::IntoClientRequest, Message};
use url::Url;

use crate::types::PolyTokenBook;
use venue_core::book::{as_f64_lenient, now_ms, parse_levels, sort_levels, upsert_level, ws_debug};
use venue_core::log::get_timestamp_ist;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Max asset IDs subscribed on a single WebSocket connection. Polymarket can silently accept a
/// very large subscription and then stream little or no book data, so we cap each connection's
/// subscription set and spread the tokens across several connections.
const CHUNK_SIZE: usize = 100;

/// After (re)connecting, every subscribed asset must deliver its initial `book` snapshot within
/// this window — Polymarket sends one snapshot per asset on connect. If any are still missing at
/// the deadline the connection is treated as frozen and force-reconnected.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(12);

/// If a live connection delivers no data frame at all for this long, force a reconnect. WS
/// ping/pong keeps working during an app-level freeze, so pongs are deliberately excluded from
/// this clock (see `read_loop`). Generous to avoid needless reconnects in genuinely quiet
/// markets; a false positive only costs a re-snapshot, which is harmless.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// Run the Polymarket market-data feed forever.
///
/// The tracked tokens are split into chunks of at most `CHUNK_SIZE`, and each chunk gets its own
/// multiplexed WebSocket connection with an independent reconnect loop. Every connection routes
/// updates by `asset_id` into the shared `books` map, so it does not matter which connection
/// delivers a given token's update. Chunking bounds each subscription (avoiding Polymarket's
/// silent-freeze-at-scale) and isolates faults: one connection dropping only blanks its own
/// tokens while the others keep streaming.
pub async fn run_poly_ws(books: Arc<HashMap<String, PolyTokenBook>>) {
    let mut asset_ids: Vec<String> = books.keys().cloned().collect();
    if asset_ids.is_empty() {
        eprintln!("[{}] : [poly-ws] no Polymarket tokens to subscribe to; not connecting", get_timestamp_ist());
        return;
    }
    asset_ids.sort(); // deterministic chunk membership + log ordering

    let chunks: Vec<Vec<String>> = asset_ids.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();
    println!(
        "[{}] : [poly-ws] {} tokens across {} connection(s) (chunk size {})",
        get_timestamp_ist(),
        asset_ids.len(),
        chunks.len(),
        CHUNK_SIZE
    );

    // One independent, self-reconnecting connection per chunk.
    let mut set = JoinSet::new();
    for (conn, chunk) in chunks.into_iter().enumerate() {
        let books = Arc::clone(&books);
        set.spawn(async move { run_one_connection(conn, chunk, books).await });
    }

    // Each connection loops forever; this only wakes if a connection task panics.
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            eprintln!("[{}] : [poly-ws] connection task ended unexpectedly: {e}", get_timestamp_ist());
        }
    }
}

/// One chunk's forever reconnect loop with capped backoff.
async fn run_one_connection(
    conn: usize,
    asset_ids: Vec<String>,
    books: Arc<HashMap<String, PolyTokenBook>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match connect_and_run(conn, &asset_ids, &books).await {
            Ok(()) => {
                println!("[{}] : [poly-ws#{conn}] connection closed; reconnecting...", get_timestamp_ist());
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                eprintln!("[{}] : [poly-ws#{conn}] {e}; reconnecting in {backoff:?}", get_timestamp_ist());
                time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        }
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn connect_and_run(
    conn: usize,
    asset_ids: &[String],
    books: &HashMap<String, PolyTokenBook>,
) -> Result<()> {
    let ws_url = Url::parse(WS_URL)?;

    // Let tungstenite generate the handshake headers (Sec-WebSocket-Key etc.);
    // we only add the extra headers Polymarket expects.
    let mut req = ws_url.as_str().into_client_request()?;
    req.headers_mut().insert(
        "Origin",
        http::HeaderValue::from_static("https://polymarket.com"),
    );
    req.headers_mut().insert(
        "User-Agent",
        http::HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
        ),
    );

    let (ws_stream, _resp) = connect_async(req).await?;
    println!("[{}] : [poly-ws#{conn}] connected; subscribing to {} tokens", get_timestamp_ist(), asset_ids.len());

    let (write, mut read) = ws_stream.split();
    let write = Arc::new(Mutex::new(write));

    let subscribe_msg = json!({
        "assets_ids": asset_ids,
        "type": "market",
    })
    .to_string();
    write.lock().await.send(Message::Text(subscribe_msg)).await?;

    // Watchdog state, updated by the read loop as frames arrive:
    //   `pending`      — assets still awaiting their initial `book` snapshot.
    //   `last_data_at` — epoch-ms of the most recent *data* frame (Text/Binary; pongs excluded).
    let pending: Arc<Mutex<HashSet<String>>> =
        Arc::new(Mutex::new(asset_ids.iter().cloned().collect()));
    let last_data_at = Arc::new(AtomicU64::new(now_ms()));

    // Keepalive: WS ping every 30s. Aborted when this connection ends.
    let ping_write = Arc::clone(&write);
    let ping_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if ping_write
                .lock()
                .await
                .send(Message::Ping(vec![]))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Run the read loop against two watchdogs. If either watchdog wins, the connection is
    // considered frozen and we return an error so `run_one_connection` reconnects + re-subscribes
    // (Polymarket re-snapshots every asset on connect, so the books self-heal).
    let result = tokio::select! {
        r = read_loop(&mut read, books, &pending, &last_data_at, conn) => r,
        missing = snapshot_watch(&pending, SNAPSHOT_TIMEOUT) => Err(anyhow!(
            "watchdog: {}/{} asset(s) never sent an initial book snapshot within {:?}: {}",
            missing.len(),
            asset_ids.len(),
            SNAPSHOT_TIMEOUT,
            describe_assets(&missing, books)
        )),
        () = idle_watch(&last_data_at, IDLE_TIMEOUT) => Err(anyhow!(
            "watchdog: no market data for {:?}",
            IDLE_TIMEOUT
        )),
    };

    ping_task.abort();
    result
}

/// Resolve once the snapshot deadline passes, returning the assets that still have not delivered
/// their initial `book`. If none are missing (subscription healthy) it parks forever, so this
/// branch of the `select!` never wins.
async fn snapshot_watch(pending: &Mutex<HashSet<String>>, deadline: Duration) -> Vec<String> {
    time::sleep(deadline).await;
    let missing: Vec<String> = pending.lock().await.iter().cloned().collect();
    if missing.is_empty() {
        std::future::pending::<()>().await;
    }
    missing
}

/// How many silent assets to name in a watchdog message before summarizing the rest. A whole
/// chunk can go quiet at once (100 tokens), and the full list would bury the log.
const MAX_NAMED_ASSETS: usize = 12;

/// Render silent asset ids as market names for the log — a bare token id says nothing about which
/// market stopped snapshotting, which is usually a market that just closed or resolved. Sorted by
/// name so repeated watchdog trips over the same markets read identically.
fn describe_assets(asset_ids: &[String], books: &HashMap<String, PolyTokenBook>) -> String {
    let mut names: Vec<String> = asset_ids
        .iter()
        .map(|id| match books.get(id) {
            Some(book) => book.label.clone(),
            // Not in the routing map: can't happen (we subscribe to its keys), so show the raw id.
            None => format!("<unknown token {id}>"),
        })
        .collect();
    names.sort();

    let extra = names.len().saturating_sub(MAX_NAMED_ASSETS);
    names.truncate(MAX_NAMED_ASSETS);
    let mut out = names.join("; ");
    if extra > 0 {
        out.push_str(&format!("; +{extra} more"));
    }
    out
}

/// Resolve once no data frame has arrived for `timeout`, polling the `last_data_at` clock that
/// the read loop stamps on every data frame. WS pongs are excluded from that clock, so an
/// app-level freeze that still answers pings is still detected.
async fn idle_watch(last_data_at: &AtomicU64, timeout: Duration) {
    let timeout_ms = timeout.as_millis() as u64;
    let mut tick = time::interval(Duration::from_secs(5));
    tick.tick().await; // interval's first tick fires immediately; skip it
    loop {
        tick.tick().await;
        if now_ms().saturating_sub(last_data_at.load(Ordering::Relaxed)) >= timeout_ms {
            return;
        }
    }
}

async fn read_loop<S>(
    read: &mut S,
    books: &HashMap<String, PolyTokenBook>,
    pending: &Mutex<HashSet<String>>,
    last_data_at: &AtomicU64,
    conn: usize,
) -> Result<()>
where
    S: StreamExt<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
{
    while let Some(msg) = read.next().await {
        let msg = msg?;
        match msg {
            Message::Text(t) => {
                last_data_at.store(now_ms(), Ordering::Relaxed);
                if ws_debug() {
                    println!("[{}] : [poly-ws#{conn}] RECV: {t}", get_timestamp_ist());
                }
                if let Err(e) = handle_message(&t, books, pending).await {
                    eprintln!("[{}] : [poly-ws#{conn}] handle error: {e}", get_timestamp_ist());
                }
            }
            Message::Binary(b) => {
                last_data_at.store(now_ms(), Ordering::Relaxed);
                if let Ok(s) = String::from_utf8(b) {
                    if ws_debug() {
                        println!("[{}] : [poly-ws#{conn}] RECV BINARY: {s}", get_timestamp_ist());
                    }
                    if let Err(e) = handle_message(&s, books, pending).await {
                        eprintln!("[{}] : [poly-ws#{conn}] handle error: {e}", get_timestamp_ist());
                    }
                }
            }
            Message::Close(frame) => {
                println!("[{}] : [poly-ws#{conn}] server closed: {frame:?}", get_timestamp_ist());
                return Ok(());
            }
            Message::Ping(_) | Message::Pong(_) => {}
            _ => {}
        }
    }
    Ok(())
}

/// Parse a frame and dispatch each contained event. Frames may be a single object or an array
/// of events; non-JSON frames (e.g. a bare "PONG") are ignored.
async fn handle_message(
    text: &str,
    books: &HashMap<String, PolyTokenBook>,
    pending: &Mutex<HashSet<String>>,
) -> Result<()> {
    let value: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    match value {
        Value::Array(items) => {
            for item in &items {
                handle_event(item, books, pending).await?;
            }
        }
        obj => handle_event(&obj, books, pending).await?,
    }
    Ok(())
}

async fn handle_event(
    event: &Value,
    books: &HashMap<String, PolyTokenBook>,
    pending: &Mutex<HashSet<String>>,
) -> Result<()> {
    match event["event_type"].as_str() {
        Some("book") => {
            let result = apply_book_snapshot(event, books).await;
            // This asset has delivered its initial snapshot, so the watchdog no longer waits on
            // it. Removing an already-removed id (later re-snapshots) is a harmless no-op.
            if let Some(asset_id) = event["asset_id"].as_str() {
                pending.lock().await.remove(asset_id);
            }
            result
        }
        Some("price_change") => apply_price_change(event, books).await,
        // tick_size_change / last_trade_price / etc. are not needed for book maintenance.
        _ => Ok(()),
    }
}

/// Full book snapshot for one asset: replace both sides.
async fn apply_book_snapshot(book: &Value, books: &HashMap<String, PolyTokenBook>) -> Result<()> {
    let asset_id = book["asset_id"].as_str().unwrap_or("");
    let Some(token) = books.get(asset_id) else {
        return Ok(());
    };

    *token.bids.lock().await = parse_levels(&book["bids"])?;
    *token.asks.lock().await = parse_levels(&book["asks"])?;

    if let Some(tick) = book["tick_size"].as_str().and_then(|t| t.parse::<f64>().ok()) {
        *token.tick_size.lock().await = tick;
    }

    sort_levels(&token.bids, &token.asks).await;
    token.change.notify_one();
    Ok(())
}

/// Incremental price/size updates. Handles both observed Polymarket shapes:
///   { "price_changes": [ { asset_id, price, size, side }, ... ] }
///   { "asset_id": "..", "changes": [ { price, size, side }, ... ] }
async fn apply_price_change(event: &Value, books: &HashMap<String, PolyTokenBook>) -> Result<()> {
    let top_asset = event["asset_id"].as_str();

    let changes: Vec<(String, &Value)> = if let Some(arr) = event["price_changes"].as_array() {
        arr.iter()
            .filter_map(|c| {
                c["asset_id"]
                    .as_str()
                    .or(top_asset)
                    .map(|a| (a.to_string(), c))
            })
            .collect()
    } else if let Some(arr) = event["changes"].as_array() {
        arr.iter()
            .filter_map(|c| top_asset.map(|a| (a.to_string(), c)))
            .collect()
    } else {
        return Ok(());
    };

    for (asset_id, change) in changes {
        let Some(token) = books.get(&asset_id) else {
            continue;
        };
        let price = as_f64_lenient(&change["price"]);
        let size = as_f64_lenient(&change["size"]);
        let side = change["side"].as_str().unwrap_or("");

        let levels = if side.eq_ignore_ascii_case("BUY") {
            &token.bids
        } else {
            &token.asks
        };
        {
            let mut guard = levels.lock().await;
            upsert_level(&mut guard, price, size);
        }

        sort_levels(&token.bids, &token.asks).await;
        token.change.notify_one();
    }

    Ok(())
}
