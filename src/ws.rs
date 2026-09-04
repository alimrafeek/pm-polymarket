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
use venue_core::log::{get_timestamp_ist, log_event};

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

/// Consecutive snapshot deadlines a token may miss before it is dropped from its connection's
/// subscription set. A token with no order book is never sent a `book` frame at all — Polymarket
/// accepts the subscription and silently ignores it — so without this the token sits in `pending`
/// forever and tears down its whole 100-token shard on every reconnect, permanently.
///
/// Five is deliberately generous: each strike costs one full connect + snapshot deadline, so a
/// token gets five independent chances across roughly three and a half minutes before it is
/// written off. Strikes are **consecutive** — one delivered snapshot clears the count — so a
/// merely flaky token never accumulates its way to a prune.
const MAX_SNAPSHOT_STRIKES: u32 = 5;

/// How long a pruned token sits out before the rehab connection re-subscribes it.
const REHAB_DELAY: Duration = Duration::from_secs(3600);

/// How often the rehab connection checks the pool while it has nothing subscribed.
const REHAB_POLL: Duration = Duration::from_secs(60);

/// Connection tag for the rehab connection, used in its log prefix (`[poly-ws#rehab]`).
const REHAB_TAG: &str = "rehab";

/// Dedicated log file for the prune/rehab lifecycle: `logs/Poly_Pruned_Tokens_<YYYY-MM-DD>.log`.
/// Not a registered log group, so it lands at the root of `logs/` alongside `General` and
/// `Balances`. Separate from the console feed on purpose — these events are rare, and a token
/// silently leaving the quoted universe is the kind of thing that needs its own greppable history
/// rather than a line buried in a shard's reconnect chatter.
const PRUNE_LOG: &str = "Poly_Pruned_Tokens";

/// Run the Polymarket market-data feed forever.
///
/// The tracked tokens are split into chunks of at most `CHUNK_SIZE`, and each chunk gets its own
/// multiplexed WebSocket connection with an independent reconnect loop. Every connection routes
/// updates by `asset_id` into the shared `books` map, so it does not matter which connection
/// delivers a given token's update. Chunking bounds each subscription (avoiding Polymarket's
/// silent-freeze-at-scale) and isolates faults: one connection dropping only stops its own tokens
/// updating while the others keep streaming. Note that a drop does **not** clear any book — only
/// [`prune`] blanks levels, so a token whose connection is down keeps its last levels until the
/// connection returns.
///
/// One extra connection is spawned beyond the chunks: the **rehab** connection, which owns no
/// tokens of its own and instead re-subscribes tokens the chunks pruned, once each has sat out
/// [`REHAB_DELAY`]. Giving rehab its own connection is the point of the design — a token that is
/// still dark after its sit-out fails there, where the only thing it can disturb is other pruned
/// tokens, instead of taking a hundred healthy ones down with it.
pub async fn run_poly_ws(books: Arc<HashMap<String, PolyTokenBook>>) {
    let mut asset_ids: Vec<String> = books.keys().cloned().collect();
    if asset_ids.is_empty() {
        eprintln!("[{}] : [poly-ws] no Polymarket tokens to subscribe to; not connecting", get_timestamp_ist());
        return;
    }
    asset_ids.sort(); // deterministic chunk membership + log ordering

    let chunks: Vec<Vec<String>> = asset_ids.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();
    println!(
        "[{}] : [poly-ws] {} tokens across {} connection(s) (chunk size {}) + 1 rehab connection",
        get_timestamp_ist(),
        asset_ids.len(),
        chunks.len(),
        CHUNK_SIZE
    );

    let pool = Arc::new(RehabPool::new());

    // One independent, self-reconnecting connection per chunk.
    let mut set = JoinSet::new();
    for (conn, chunk) in chunks.into_iter().enumerate() {
        let books = Arc::clone(&books);
        let pool = Arc::clone(&pool);
        set.spawn(async move { run_one_connection(conn.to_string(), chunk, books, pool, false).await });
    }

    // The rehab connection starts empty and fills from the pool as sit-outs expire.
    set.spawn(async move {
        run_one_connection(REHAB_TAG.to_string(), Vec::new(), books, pool, true).await
    });

    // Each connection loops forever; this only wakes if a connection task panics.
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            eprintln!("[{}] : [poly-ws] connection task ended unexpectedly: {e}", get_timestamp_ist());
        }
    }
}

/// Tokens pruned from a chunk for repeatedly failing to snapshot, each stamped with the instant it
/// was pruned. Shared by every connection: the chunks put tokens in, the rehab connection takes
/// them out once [`REHAB_DELAY`] has elapsed.
///
/// A token that is still dark in rehab is pruned again with a fresh stamp, so a permanently dead
/// market costs one short connection attempt per hour and never touches a healthy chunk again.
struct RehabPool {
    /// `asset_id` → epoch-ms it was pruned at.
    parked: Mutex<HashMap<String, u64>>,
}

impl RehabPool {
    fn new() -> Self {
        Self { parked: Mutex::new(HashMap::new()) }
    }

    /// Park `asset_ids` as of `at_ms`. Re-parking a token already in the pool restamps it, which
    /// is what restarts the sit-out for one that failed its rehab attempt.
    async fn park_at(&self, asset_ids: &[String], at_ms: u64) {
        let mut guard = self.parked.lock().await;
        for id in asset_ids {
            guard.insert(id.clone(), at_ms);
        }
    }

    async fn park(&self, asset_ids: &[String]) {
        self.park_at(asset_ids, now_ms()).await;
    }

    /// Remove and return up to `max` tokens whose sit-out has elapsed, longest-parked first.
    ///
    /// The cap keeps the rehab connection inside `CHUNK_SIZE` even if a large slice of the
    /// universe dies at once — the whole reason chunking exists is that a very large subscription
    /// gets silently ignored, and rehab must not be the one place that re-creates it. Whatever
    /// does not fit stays parked with its original stamp, so it is still overdue and comes out on
    /// the next tick rather than waiting another hour.
    async fn take_due(&self, delay: Duration, max: usize) -> Vec<String> {
        let cutoff = now_ms().saturating_sub(delay.as_millis() as u64);
        let mut guard = self.parked.lock().await;

        let mut due: Vec<(u64, String)> = guard
            .iter()
            .filter(|(_, &at)| at <= cutoff)
            .map(|(id, &at)| (at, id.clone()))
            .collect();
        due.sort(); // (parked_at, asset_id) — oldest first, ties broken deterministically
        due.truncate(max);

        let ids: Vec<String> = due.into_iter().map(|(_, id)| id).collect();
        for id in &ids {
            guard.remove(id);
        }
        ids
    }

    async fn len(&self) -> usize {
        self.parked.lock().await.len()
    }
}

/// One connection's forever reconnect loop with capped backoff, plus the strike accounting that
/// decides when a token stops being worth subscribing to.
///
/// `asset_ids` is the *starting* subscription set, not a fixed one: tokens leave it when they hit
/// [`MAX_SNAPSHOT_STRIKES`], and — on the rehab connection (`intake`) — join it as sit-outs
/// expire. Everything else about the loop is unchanged.
///
/// A recovered token is deliberately **not** handed back to its original chunk. Books are routed
/// by `asset_id`, so which connection delivers an update does not matter, and moving a live token
/// between connections would cost a tear-down on both.
async fn run_one_connection(
    conn: String,
    asset_ids: Vec<String>,
    books: Arc<HashMap<String, PolyTokenBook>>,
    pool: Arc<RehabPool>,
    intake: bool,
) {
    let mut live = asset_ids;
    // Consecutive missed snapshots per token. An id is absent when its count is zero, so a healthy
    // connection keeps this map empty.
    let mut strikes: HashMap<String, u32> = HashMap::new();
    // Rehab only: tokens already reported as recovered, so one recovery logs one line.
    let mut recovered: HashSet<String> = HashSet::new();
    let mut backoff = Duration::from_secs(1);

    loop {
        if intake {
            let due = pool.take_due(REHAB_DELAY, CHUNK_SIZE.saturating_sub(live.len())).await;
            if !due.is_empty() {
                announce(
                    &conn,
                    &format!(
                        "re-subscribing {} token(s) after a {:?} sit-out: {}",
                        due.len(),
                        REHAB_DELAY,
                        describe_assets(&due, &books, usize::MAX)
                    ),
                );
                live.extend(due);
                live.sort();
                live.dedup();
            }
        }

        // Normally only the rehab connection, which is idle until something is pruned — but a
        // chunk that loses every one of its tokens lands here too and idles until they are
        // rehabilitated onto the rehab connection. Either way there is nothing to subscribe to.
        if live.is_empty() {
            time::sleep(REHAB_POLL).await;
            continue;
        }

        // Filled in by `snapshot_watch` when the deadline fires, and only then: a socket that died
        // before the deadline tells us nothing about any token, and must not cost anyone a strike.
        let verdict: Mutex<Option<HashSet<String>>> = Mutex::new(None);
        let outcome = connect_and_run(&conn, &live, &books, &verdict).await;

        if let Some(missing) = verdict.into_inner() {
            // Stop quoting anything the venue declined to confirm, now rather than at the fifth
            // strike. Blanking an already-empty book is a no-op, so this costs nothing on a token
            // that never had levels; it exists for the one that was healthy for hours and then
            // went dark, which is the case that can put a real but stale price in front of the
            // arb engine.
            blank_books(&books_to_blank(&missing, live.len()), &books).await;

            if intake {
                report_recoveries(&conn, &live, &missing, &mut recovered, &books);
            }
            let pruned = apply_strikes(&mut strikes, &live, &missing, MAX_SNAPSHOT_STRIKES);
            if !pruned.is_empty() {
                let dropped: HashSet<&String> = pruned.iter().collect();
                live.retain(|id| !dropped.contains(id));
                recovered.retain(|id| !dropped.contains(id));
                // Blank before parking: an unsubscribed token receives no further updates, so
                // whatever levels it holds would otherwise stay quotable forever.
                blank_books(&pruned, &books).await;
                pool.park(&pruned).await;
                announce(
                    &conn,
                    &format!(
                        "pruned {} token(s) after {MAX_SNAPSHOT_STRIKES} consecutive missed snapshots; \
                         books blanked, {} still subscribed here, rehab pool now holds {}: {}",
                        pruned.len(),
                        live.len(),
                        pool.len().await,
                        describe_assets(&pruned, &books, usize::MAX)
                    ),
                );
            }
        }

        match outcome {
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

/// Which books to clear when a snapshot deadline finds `missing` of `total` subscribed assets
/// silent. Sorted, and empty when there is nothing to do.
///
/// A **partial** miss is a statement about tokens: the connection demonstrably works — other
/// assets on it snapshotted — so the ones that stayed silent are markets the venue has stopped
/// confirming, and their levels must stop being quotable immediately.
///
/// A miss of **everything** is a statement about the socket, not about any token: the subscription
/// was silently ignored, which is the failure the chunking and the watchdog exist for. It says
/// nothing about any individual market, and the reconnect a second later re-snapshots the lot — so
/// blanking here would take a whole shard's markets dark on every transient subscribe hiccup, for
/// no information gained. Those books are left alone, and the prune blanks them later if the
/// silence turns out to be real.
fn books_to_blank(missing: &HashSet<String>, total: usize) -> Vec<String> {
    if missing.is_empty() || missing.len() >= total {
        return Vec::new();
    }
    let mut silent: Vec<String> = missing.iter().cloned().collect();
    silent.sort();
    silent
}

/// Apply one snapshot verdict to the running strike counts, returning the tokens that just reached
/// `limit` — sorted, and cleared from `strikes` so a token returning from rehab starts fresh.
///
/// Every id in `live` is accounted for: present in `missing` costs a strike, absent clears the
/// count outright. That reset is what makes the count **consecutive** rather than cumulative — a
/// token that misses four deadlines and then delivers one is back to zero, not one strike from
/// being pruned.
fn apply_strikes(
    strikes: &mut HashMap<String, u32>,
    live: &[String],
    missing: &HashSet<String>,
    limit: u32,
) -> Vec<String> {
    let mut pruned = Vec::new();
    for id in live {
        if !missing.contains(id) {
            strikes.remove(id);
            continue;
        }
        let count = strikes.entry(id.clone()).or_insert(0);
        *count += 1;
        if *count >= limit {
            pruned.push(id.clone());
        }
    }
    for id in &pruned {
        strikes.remove(id);
    }
    pruned.sort();
    pruned
}

/// Blank the books of tokens leaving the subscription set, so nothing quotes levels the feed has
/// stopped maintaining. Empty levels make `best_ask`/`best_bid` return `None`, which is how the
/// arb engine already expresses "no market here".
///
/// `tick_size` is left alone — it is market metadata rather than a quote, and re-reading it would
/// cost a REST call for no gain. Notifying on a removal is deliberate: a consumer parked on
/// `change` should learn the quotes went away rather than hold a view the feed has abandoned.
async fn blank_books(asset_ids: &[String], books: &HashMap<String, PolyTokenBook>) {
    for id in asset_ids {
        let Some(token) = books.get(id) else { continue };
        token.bids.lock().await.clear();
        token.asks.lock().await.clear();
        token.change.notify_one();
    }
}

/// Log the rehab tokens that snapshotted this cycle, once each. This is the line that answers
/// whether a pruned token ever comes back on its own; if it never appears, prunes are permanent in
/// practice and the sit-out is only costing a connection attempt an hour.
fn report_recoveries(
    conn: &str,
    live: &[String],
    missing: &HashSet<String>,
    recovered: &mut HashSet<String>,
    books: &HashMap<String, PolyTokenBook>,
) {
    let fresh: Vec<String> = live
        .iter()
        .filter(|id| !missing.contains(*id) && !recovered.contains(*id))
        .cloned()
        .collect();
    if fresh.is_empty() {
        return;
    }
    recovered.extend(fresh.iter().cloned());
    announce(
        conn,
        &format!(
            "{} token(s) recovered and are streaming again: {}",
            fresh.len(),
            describe_assets(&fresh, books, usize::MAX)
        ),
    );
}

/// Write one prune-lifecycle event to both the console and [`PRUNE_LOG`]. The console copy keeps
/// the operator's live view honest about the universe shrinking; the file copy is the history,
/// and is the only one that carries every name rather than a truncated sample.
fn announce(conn: &str, event: &str) {
    eprintln!("[{}] : [poly-ws#{conn}] {event}", get_timestamp_ist());
    log_event(PRUNE_LOG, &format!("[poly-ws#{conn}] {event}"));
}

async fn connect_and_run(
    conn: &str,
    asset_ids: &[String],
    books: &HashMap<String, PolyTokenBook>,
    verdict: &Mutex<Option<HashSet<String>>>,
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
        missing = snapshot_watch(&pending, SNAPSHOT_TIMEOUT, verdict) => Err(anyhow!(
            "watchdog: {}/{} asset(s) never sent an initial book snapshot within {:?}{}: {}",
            missing.len(),
            asset_ids.len(),
            SNAPSHOT_TIMEOUT,
            // `run_one_connection` blanks these on the way out — but only on a partial miss; an
            // all-miss is a socket failure and leaves the books for the reconnect. See
            // [`books_to_blank`].
            if missing.len() < asset_ids.len() { ", books blanked" } else { "" },
            describe_assets(&missing, books, MAX_NAMED_ASSETS)
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
///
/// Either way it records the outcome in `verdict` first. The healthy case has to be recorded too,
/// even though it resolves nothing here: it is what clears the strike counts of every token that
/// did deliver, and without it a count would accumulate across unrelated failures instead of
/// consecutive ones.
async fn snapshot_watch(
    pending: &Mutex<HashSet<String>>,
    deadline: Duration,
    verdict: &Mutex<Option<HashSet<String>>>,
) -> Vec<String> {
    time::sleep(deadline).await;
    let missing: HashSet<String> = pending.lock().await.clone();
    *verdict.lock().await = Some(missing.clone());

    if missing.is_empty() {
        std::future::pending::<()>().await;
    }
    let mut missing: Vec<String> = missing.into_iter().collect();
    missing.sort();
    missing
}

/// How many silent assets to name in a watchdog message before summarizing the rest. A whole
/// chunk can go quiet at once (100 tokens), and the full list would bury the log.
const MAX_NAMED_ASSETS: usize = 12;

/// Render silent asset ids as market names for the log — a bare token id says nothing about which
/// market stopped snapshotting, which is usually a market that just closed or resolved. Sorted by
/// name so repeated watchdog trips over the same markets read identically.
///
/// `limit` caps how many are named before the rest are summarised as a count: the reconnect
/// chatter passes [`MAX_NAMED_ASSETS`], while the prune log passes `usize::MAX` — a permanent
/// change to the quoted universe is worth every name, however long the list.
fn describe_assets(
    asset_ids: &[String],
    books: &HashMap<String, PolyTokenBook>,
    limit: usize,
) -> String {
    let mut names: Vec<String> = asset_ids
        .iter()
        .map(|id| match books.get(id) {
            Some(book) => book.label.clone(),
            // Not in the routing map: can't happen (we subscribe to its keys), so show the raw id.
            None => format!("<unknown token {id}>"),
        })
        .collect();
    names.sort();

    let extra = names.len().saturating_sub(limit);
    names.truncate(limit);
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
    conn: &str,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;
    use venue_core::book::OrderBookLevel;

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn missing_set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn book_with_levels(label: &str) -> PolyTokenBook {
        PolyTokenBook {
            label: label.to_string(),
            bids: Arc::new(Mutex::new(vec![OrderBookLevel { price: 0.40, size: 100.0 }])),
            asks: Arc::new(Mutex::new(vec![OrderBookLevel { price: 0.60, size: 100.0 }])),
            tick_size: Arc::new(Mutex::new(0.01)),
            change: Arc::new(Notify::new()),
        }
    }

    #[test]
    fn a_missed_deadline_strikes_only_the_missing_tokens() {
        let mut strikes = HashMap::new();
        let pruned = apply_strikes(&mut strikes, &ids(&["a", "b", "c"]), &missing_set(&["b"]), 5);

        assert!(pruned.is_empty(), "one strike is nowhere near the limit");
        assert_eq!(strikes.get("b"), Some(&1));
        assert!(strikes.get("a").is_none(), "a delivered, so it carries no count");
        assert!(strikes.get("c").is_none());
    }

    /// The invariant the whole policy rests on: strikes are consecutive, not cumulative. Four
    /// misses followed by one delivered snapshot puts the token back to zero, so the next miss is
    /// strike one — a token that is merely flaky can never accumulate its way to a prune.
    #[test]
    fn one_delivered_snapshot_clears_the_count() {
        let mut strikes = HashMap::new();
        let live = ids(&["a"]);
        for _ in 0..4 {
            assert!(apply_strikes(&mut strikes, &live, &missing_set(&["a"]), 5).is_empty());
        }
        assert_eq!(strikes.get("a"), Some(&4), "one strike short of the limit");

        // The snapshot lands.
        assert!(apply_strikes(&mut strikes, &live, &missing_set(&[]), 5).is_empty());
        assert!(strikes.get("a").is_none(), "the count is cleared, not decremented");

        // ...and the very next miss starts over rather than tipping it over the limit.
        assert!(apply_strikes(&mut strikes, &live, &missing_set(&["a"]), 5).is_empty());
        assert_eq!(strikes.get("a"), Some(&1));
    }

    #[test]
    fn the_fifth_consecutive_miss_prunes_and_resets() {
        let mut strikes = HashMap::new();
        let live = ids(&["a", "b"]);
        for _ in 0..4 {
            assert!(apply_strikes(&mut strikes, &live, &missing_set(&["b", "a"]), 5).is_empty());
        }

        let pruned = apply_strikes(&mut strikes, &live, &missing_set(&["b", "a"]), 5);
        assert_eq!(pruned, ids(&["a", "b"]), "returned sorted");
        assert!(strikes.is_empty(), "a pruned token starts fresh if rehab returns it");
    }

    #[test]
    fn a_healthy_deadline_prunes_nothing_and_leaves_no_state() {
        let mut strikes = HashMap::new();
        let pruned = apply_strikes(&mut strikes, &ids(&["a", "b"]), &missing_set(&[]), 5);
        assert!(pruned.is_empty());
        assert!(strikes.is_empty(), "a healthy connection keeps the map empty");
    }

    /// Tokens that left `live` (pruned, or moved to rehab) must not keep striking from the
    /// sidelines — only ids in `live` are accounted for.
    #[test]
    fn tokens_outside_the_live_set_are_untouched() {
        let mut strikes = HashMap::new();
        strikes.insert("gone".to_string(), 3);
        apply_strikes(&mut strikes, &ids(&["a"]), &missing_set(&["a", "gone"]), 5);
        assert_eq!(strikes.get("gone"), Some(&3), "unchanged: it is not in `live`");
    }

    /// The connection works and named the tokens that did not answer, so those stop being
    /// quotable at once — four strikes before the prune would otherwise get around to it.
    #[test]
    fn a_partial_miss_blanks_exactly_the_silent_tokens() {
        assert_eq!(books_to_blank(&missing_set(&["c", "a"]), 3), ids(&["a", "c"]));
    }

    /// Zero snapshots is a broken socket, not a statement about any market. Blanking here would
    /// take a whole shard dark on every transient subscribe hiccup, so it is deliberately left to
    /// the reconnect — and to the prune, if the silence turns out to be real.
    #[test]
    fn an_all_silent_verdict_blanks_nothing() {
        assert!(books_to_blank(&missing_set(&["a", "b", "c"]), 3).is_empty());
    }

    #[test]
    fn a_healthy_verdict_blanks_nothing() {
        assert!(books_to_blank(&missing_set(&[]), 3).is_empty());
    }

    #[tokio::test]
    async fn a_parked_token_is_withheld_until_its_sit_out_expires() {
        let pool = RehabPool::new();
        pool.park(&ids(&["fresh"])).await;

        assert!(
            pool.take_due(REHAB_DELAY, CHUNK_SIZE).await.is_empty(),
            "just parked — nowhere near an hour old"
        );
        assert_eq!(pool.len().await, 1, "and it stays in the pool");

        // Backdate it past the sit-out.
        pool.park_at(&ids(&["fresh"]), now_ms() - REHAB_DELAY.as_millis() as u64 - 1).await;
        assert_eq!(pool.take_due(REHAB_DELAY, CHUNK_SIZE).await, ids(&["fresh"]));
        assert_eq!(pool.len().await, 0, "taking it removes it — it cannot be issued twice");
    }

    #[tokio::test]
    async fn take_due_honours_the_cap_and_leaves_the_rest_overdue() {
        let pool = RehabPool::new();
        let old = now_ms() - REHAB_DELAY.as_millis() as u64 - 10_000;
        // Distinct stamps, parked out of order, so "longest parked first" is a real assertion.
        pool.park_at(&ids(&["c"]), old + 20).await;
        pool.park_at(&ids(&["a"]), old).await;
        pool.park_at(&ids(&["b"]), old + 10).await;

        assert_eq!(pool.take_due(REHAB_DELAY, 2).await, ids(&["a", "b"]));
        assert_eq!(pool.len().await, 1, "the capped-out token stays parked");
        // It is still overdue, so it comes out on the very next tick rather than waiting an hour.
        assert_eq!(pool.take_due(REHAB_DELAY, 2).await, ids(&["c"]));
    }

    /// A token that fails its rehab attempt is re-parked, and the restamp is what buys the next
    /// full sit-out instead of it coming due again immediately.
    #[tokio::test]
    async fn re_parking_restarts_the_sit_out() {
        let pool = RehabPool::new();
        pool.park_at(&ids(&["x"]), now_ms() - REHAB_DELAY.as_millis() as u64 - 1).await;
        pool.park(&ids(&["x"])).await;

        assert!(pool.take_due(REHAB_DELAY, CHUNK_SIZE).await.is_empty());
        assert_eq!(pool.len().await, 1, "restamped, not duplicated");
    }

    #[tokio::test]
    async fn blanking_clears_quotes_but_not_metadata_or_neighbours() {
        let books: HashMap<String, PolyTokenBook> = [
            ("dead".to_string(), book_with_levels("Dead / YES")),
            ("live".to_string(), book_with_levels("Live / YES")),
        ]
        .into_iter()
        .collect();

        blank_books(&ids(&["dead", "not-in-the-map"]), &books).await;

        let dead = &books["dead"];
        assert!(dead.bids.lock().await.is_empty(), "no bid survives — nothing quotes it");
        assert!(dead.asks.lock().await.is_empty());
        assert_eq!(*dead.tick_size.lock().await, 0.01, "metadata is not a quote");

        let live = &books["live"];
        assert_eq!(live.bids.lock().await.len(), 1, "an unrelated token is untouched");
        assert_eq!(live.asks.lock().await.len(), 1);
    }

    /// A `book` frame with empty sides is the venue **confirming** the market is empty, and must
    /// clear whatever the book was holding — the snapshot assignment is wholesale, never a merge.
    ///
    /// Pinned here because it rests on a detail of another crate: `parse_levels` returns
    /// `Ok(vec![])` for a missing or non-array field rather than an error. Harden that into an
    /// `Err` and `apply_book_snapshot`'s `?` would return early, leaving stale levels quotable —
    /// and an error on `asks` alone would leave the book half applied, with new bids against old
    /// asks. That is a crossed book the arb engine would happily price.
    #[tokio::test]
    async fn an_empty_snapshot_clears_a_populated_book() {
        let books: HashMap<String, PolyTokenBook> =
            [("t".to_string(), book_with_levels("Market / YES"))].into_iter().collect();

        apply_book_snapshot(
            &json!({ "event_type": "book", "asset_id": "t", "bids": [], "asks": [] }),
            &books,
        )
        .await
        .unwrap();

        assert!(books["t"].bids.lock().await.is_empty(), "the venue said empty");
        assert!(books["t"].asks.lock().await.is_empty());
        assert_eq!(*books["t"].tick_size.lock().await, 0.01, "metadata is not a quote");
    }

    /// Same rule when the venue omits the sides entirely instead of sending empty arrays.
    #[tokio::test]
    async fn a_snapshot_omitting_both_sides_clears_the_book() {
        let books: HashMap<String, PolyTokenBook> =
            [("t".to_string(), book_with_levels("Market / YES"))].into_iter().collect();

        apply_book_snapshot(&json!({ "event_type": "book", "asset_id": "t" }), &books)
            .await
            .unwrap();

        assert!(books["t"].bids.lock().await.is_empty());
        assert!(books["t"].asks.lock().await.is_empty());
    }

    /// A blank snapshot still counts as delivered: the subscription demonstrably works, so the
    /// token is healthy, keeps its place, and never strikes toward a prune. Silence and a
    /// confirmed-empty book are different signals and must not collapse into one.
    #[tokio::test]
    async fn a_blank_snapshot_still_clears_the_watchdog() {
        let books: HashMap<String, PolyTokenBook> =
            [("t".to_string(), book_with_levels("Market / YES"))].into_iter().collect();
        let pending: Mutex<HashSet<String>> = Mutex::new(missing_set(&["t"]));

        handle_event(
            &json!({ "event_type": "book", "asset_id": "t", "bids": [], "asks": [] }),
            &books,
            &pending,
        )
        .await
        .unwrap();

        assert!(pending.lock().await.is_empty(), "it answered, so it is not silent");
    }

    /// The prune log gets every name; the reconnect chatter gets a capped sample plus a count.
    #[test]
    fn describe_assets_caps_only_when_asked_to() {
        let books: HashMap<String, PolyTokenBook> = ["c", "a", "b"]
            .iter()
            .map(|id| (id.to_string(), book_with_levels(&format!("Market {id}"))))
            .collect();
        let all = ids(&["a", "b", "c"]);

        assert_eq!(
            describe_assets(&all, &books, usize::MAX),
            "Market a; Market b; Market c",
            "sorted by label, nothing elided"
        );
        assert_eq!(describe_assets(&all, &books, 2), "Market a; Market b; +1 more");
    }
}
