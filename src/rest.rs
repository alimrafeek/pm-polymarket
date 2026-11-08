use serde_json::{Map, Value};
use anyhow::{Context as _, Result, anyhow};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::types::{PolymarketFeeSchedule, PolymarketMarketDetails, DEFAULT_MIN_ORDER_SIZE};
use venue_core::log::{get_timestamp_ist, log_event, MarketSkip};
use venue_core::trade::{marked_total, MarkSource, MarkedPosition, PortfolioValue};

fn get_value(obj: &Value, key: &str) -> Value {
    obj.get(key).cloned().unwrap_or(Value::Null)
}

fn pick_fields(obj: &Value, fields: &[&str]) -> Value {
    let mut map = Map::new();

    for field in fields {
        map.insert(field.to_string(), get_value(obj, field));
    }

    Value::Object(map)
}

/// Parse a Polymarket ISO-8601 UTC timestamp like `"2026-07-20T00:00:00Z"` into Unix epoch seconds.
/// `endDate` is always UTC (trailing `Z`); any timezone offset or fractional seconds are dropped.
/// Returns `None` if the string isn't in the expected shape. Uses Howard Hinnant's days-from-civil
/// algorithm, so no date library (chrono) dependency is needed.
pub fn iso8601_to_epoch(s: &str) -> Option<i64> {
    let (date, time) = s.trim().split_once('T')?;

    let mut dp = date.split('-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;

    // Strip the `Z`, any `+hh:mm` offset, and any fractional seconds before splitting h:m:s.
    let time = time.trim_end_matches('Z');
    let time = time.split('+').next().unwrap_or(time);
    let time = time.split('.').next().unwrap_or(time);
    let mut tp = time.split(':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let min: i64 = tp.next().unwrap_or("0").parse().ok()?;
    let sec: i64 = tp.next().unwrap_or("0").parse().ok()?;

    // days_from_civil: days since 1970-01-01 for a proleptic Gregorian date.
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days = era * 146097 + doe - 719468;

    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

/// Market-level fields kept by the Gamma event filter (everything else is dropped by
/// `pick_fields` before `parse_market_detail` runs). `feeSchedule` must stay in this list: it is
/// the only place the per-market taker-fee curve exists — `feesEnabled`/`feeType` alone carry no
/// rate, and dropping it silently zeroes the Polymarket fee leg in the arbitrage edge math. `slug`
/// is the market's own address slug, kept for the alert notifier's links (`url_slug`). `question`
/// names a sports event's main market, which has no `groupItemTitle` (TEAM_LINKS_SPEC TL§3.3).
const MARKET_FIELDS: [&str; 18] = [
    "acceptingOrders",
    "active",
    "archived",
    "clobTokenIds",
    "closed",
    "conditionId",
    "endDate",
    "feeSchedule",
    "feeType",
    "feesEnabled",
    "groupItemTitle",
    "id",
    "negRisk",
    "orderPriceMinTickSize",
    "outcomePrices",
    "outcomes",
    "question",
    "slug",
];

/// Gamma event lookup for one config slug, wrapped in the venue-standard read retry (3 attempts,
/// 2 s apart) so a transient Gamma failure doesn't drop the whole Polymarket leg for the run.
///
/// Returns the tradeable markets **and** the outcomes this event carried that were dropped on the
/// way (see [`MarketSkip`]) — the caller's startup report names them, since a dropped outcome is
/// indistinguishable from one the venue never listed once it is off this Vec.
pub async fn get_poly_market_data(
    slug: String,
) -> Result<(Vec<PolymarketMarketDetails>, Vec<MarketSkip>)> {
    const MAX_ATTEMPTS: u32 = 3;
    const RETRY_DELAY_SEC: u64 = 2;

    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match fetch_poly_market_data(&slug).await {
            Ok(details) => return Ok(details),
            Err(e) => {
                log_event(
                    "General",
                    &format!("get_poly_market_data attempt {attempt}/{MAX_ATTEMPTS} failed for slug '{slug}': {e:#}"),
                );
                last_err = Some(e);
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SEC)).await;
                }
            }
        }
    }

    let err = last_err.unwrap_or_else(|| anyhow!("get_poly_market_data: no attempts were made"));
    let msg = format!(
        "get_poly_market_data : max attempts ({MAX_ATTEMPTS}) reached for slug '{slug}', giving up: {err:#}"
    );
    println!("[{}] : {msg}", get_timestamp_ist());
    log_event("General", &msg);
    Err(err).context(format!("get_poly_market_data('{slug}') failed after {MAX_ATTEMPTS} attempts"))
}

/// One attempt of [`get_poly_market_data`].
async fn fetch_poly_market_data(
    slug: &str,
) -> Result<(Vec<PolymarketMarketDetails>, Vec<MarketSkip>)> {
    let url = format!(
        "https://gamma-api.polymarket.com/events?slug={}",
        slug
    );

    let client = reqwest::Client::new();

    let response = client
        .get(&url)
        .header("accept", "application/json")
        .send()
        .await?;

    if !response.status().is_success() {
        println!("[{}] : Failed to fetch data for slug '{}'. Status: {}", get_timestamp_ist(), slug, response.status());
        return Err(anyhow!("Request failed with status: {}", response.status()));
    }

    let json_response: Value = response.json().await?;

    let event_fields = [
        "active",
        "archived",
        "automaticallyActive",
        "automaticallyResolved",
        "cantEstimate",
        "closed",
        "closedTime",
        "commentCount",
        "createdAt",
        "creationDate",
        "cyom",
        "deploying",
        "deployingTimestamp",
        "enableNegRisk",
        "enableOrderBook",
        "endDate",
        "estimateValue",
        "eventMetadata",
        "featured",
        "gmpChartMode",
        "icon",
        "id",
        "image",
        "negRisk",
        "negRiskAugmented",
        "negRiskMarketID",
        "new",
        "openInterest",
        "pendingDeployment",
        "requiresTranslation",
        "resolutionSource",
        "restricted",
        "series",
        "seriesSlug",
        "showAllOutcomes",
        "showMarketImages",
        "slug",
        "startDate",
        "tags",
        "ticker",
        "title",
        "updatedAt",
        "volume",
        "volume1mo",
        "volume1wk",
        "volume1yr",
    ];

    let mut final_events = Vec::new();

    if let Some(events) = json_response.as_array() {
        for event in events {
            let mut event_map = match pick_fields(event, &event_fields) {
                Value::Object(map) => map,
                _ => Map::new(),
            };

            let mut filtered_markets = Vec::new();

            if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                for market in markets {
                    filtered_markets.push(pick_fields(market, &MARKET_FIELDS));
                }
            }

            event_map.insert("markets".to_string(), Value::Array(filtered_markets));

            final_events.push(Value::Object(event_map));
        }
    }

    if final_events.is_empty() {
        return Err(anyhow!("No events found for slug '{}'", slug));
    }
    let market_data = final_events[0].clone();
    parse_market_detail(market_data).await
}

/// Rows per `GET /positions` page: Data API v2's ceiling, so this is one request for any realistic
/// account and the cursor walk below is insurance rather than a hot path.
const POSITIONS_PAGE_LIMIT: usize = 1000;

/// Most pages one walk reads before giving up: 50,000 rows at the page cap, far past any real
/// account. Reaching it is an `Err`, never a silent stop, because a truncated read looks like a
/// flat account (see [`get_poly_positions`]).
const POSITIONS_MAX_PAGES: usize = 50;

/// The `status` of each `/positions` pass; `None` leaves the parameter off, which is v2's default.
///
/// One pass, from the owner's H-A check against the live wallet (POLY_DATA_V2_SPEC.md §3,
/// 2026-11-03): the default list already carries resolved-but-unredeemed rows (`REDEEMABLE`),
/// neither `status=REDEEMABLE` nor `status=REDEEMABLE_LOST` showed a row it did not, and
/// `include_archived=true` added none. The check matters because a list that dropped 40 unredeemed
/// winning shares would read the account 40 USDC short, and the drawdown breaker trips on that.
const POSITIONS_PASSES: [Option<&str>; 1] = [None];

/// Current Polymarket positions for the `POLY_FUNDER` deposit wallet, as
/// `(token_id, shares, avg_price)` tuples — `token_id`, `current_size`, and `avg_price` from the
/// data API; everything else in the response is dropped. `avg_price` is the per-share
/// cost basis used to seed the Polymarket leg's entry price on restart.
///
/// Walked by cursor until `next_cursor` is `null`, because a truncated read is not a visibly
/// degraded one: `seed_positions` treats a token it never saw as flat, and a leg that looks flat
/// is handed a full `MAX_PAIRS_PER_TRADE` of fresh capacity on top of inventory it already holds
/// — and its real holdings are never offered for exit. Positions below `filter_amount` (1 share)
/// are still dropped by the venue, which is dust against a 40-pair cap.
pub async fn get_poly_positions(
    trader: &crate::trade::PolyTrader,
) -> Result<Vec<(String, f64, f64)>> {
    fetch_poly_position_rows(trader)
        .await?
        .iter()
        .map(|position| {
            let token_id = position
                .get("token_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("position missing token_id"))?;
            let size = position
                .get("current_size")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("position missing current_size"))?;
            let avg_price = position
                .get("avg_price")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("position missing avg_price"))?;
            Ok((token_id.to_string(), size, avg_price))
        })
        .collect()
}

/// Every `GET /positions` row for this trader's deposit wallet, untouched. Shared by
/// [`get_poly_positions`] and [`get_poly_portfolio`] so the cursor walk — and the de-duplication
/// and bounds that keep it from spinning or stopping short — exists once rather than once per
/// reader.
///
/// **The wallet is not named here, and cannot be.** This asks for `/positions` and the credential
/// holder appends `?user=<funder>` ([`crate::trade::PolyL2Auth::url`]), so a gateway-backed engine
/// is asking about its own account by construction. Until 2026-09-18 this built the whole URL
/// itself from `POLY_FUNDER` in the process environment and sent it on a private `reqwest` client —
/// which is why the engine still had to be handed a wallet address, and why this one read sat
/// outside the fleet rate budget that every other venue call goes through.
async fn fetch_poly_position_rows(trader: &crate::trade::PolyTrader) -> Result<Vec<Value>> {
    walk_position_pages(|path| async move {
        let response = trader.data_get(&path).await?;

        if !(200..300).contains(&response.status) {
            let msg = format!(
                "GET {path} failed: HTTP {}: {}",
                response.status, response.body,
            );
            println!("[{}] : {msg}", get_timestamp_ist());
            return Err(anyhow!(msg));
        }
        Ok(response.body)
    })
    .await
}

/// The cursor walk behind [`fetch_poly_position_rows`], over any page source so it can be tested
/// without a network: `fetch` takes a `/positions` path and returns a 2xx body.
///
/// Each pass in [`POSITIONS_PASSES`] is read until `pagination.next_cursor` is `null`, and **only**
/// then: a short or even empty page that still carries a cursor is read on. Say the wallet holds
/// 1,500 tokens: page one is 1,000 rows and cursor `abc`, page two (the same query plus
/// `cursor=abc`) is 500 rows and a `null` cursor, and that is the end.
///
/// A cursor seen twice, or a walk past [`POSITIONS_MAX_PAGES`], is an `Err`: the first would spin
/// and the second would end as a silent truncation. Rows are de-duplicated on `token_id` across
/// pages and passes — one token can only be held once, so a repeat is a row already read, and is
/// skipped.
async fn walk_position_pages<F, Fut>(mut fetch: F) -> Result<Vec<Value>>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let mut result: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for status in POSITIONS_PASSES {
        let mut cursors: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;
        loop {
            if pages == POSITIONS_MAX_PAGES {
                return Err(anyhow!(
                    "positions walk reached {POSITIONS_MAX_PAGES} pages with a cursor still open; \
                     refusing a truncated read"
                ));
            }
            pages += 1;

            let body = fetch(positions_path(status, cursor.as_deref())).await?;
            let (rows, next_cursor) = parse_positions_page(&body)
                .with_context(|| format!("parsing positions page {pages}"))?;

            for position in rows {
                let token_id = position
                    .get("token_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("position missing token_id"))?
                    .to_string();
                if !seen.insert(token_id) {
                    continue;
                }
                result.push(position);
            }

            match next_cursor {
                None => break,
                Some(next) => {
                    if !cursors.insert(next.clone()) {
                        return Err(anyhow!(
                            "positions walk was handed the same cursor twice on page {pages}"
                        ));
                    }
                    cursor = Some(next);
                }
            }
        }
    }
    Ok(result)
}

/// The `/positions` path for one page of one pass. Every page repeats the whole query, because the
/// v2 cursor carries none of it. **Never `offset`, not even `0`:** v2 answers it with HTTP 400. The
/// cursor is opaque and signed and may hold `+`, `/` or `=`, so it is percent-encoded.
fn positions_path(status: Option<&str>, cursor: Option<&str>) -> String {
    let mut path = format!(
        "/positions?filter_type=TOKENS&filter_amount=1&sort_by=TOKENS&sort_direction=DESC&limit={POSITIONS_PAGE_LIMIT}"
    );
    if let Some(status) = status {
        path.push_str("&status=");
        path.push_str(status);
    }
    if let Some(cursor) = cursor {
        path.push_str("&cursor=");
        path.extend(url::form_urlencoded::byte_serialize(cursor.as_bytes()));
    }
    path
}

/// One v2 `/positions` body, `{"data": [...], "pagination": {"next_cursor": ...}}`, as its rows and
/// its next cursor (`None` on the last page).
///
/// A `null` or empty `data` is no positions, the contract's documented miss, not an error. Anything
/// else that is not a `data` array — or a missing `pagination` object or `next_cursor` — is an
/// `Err` naming what was missing: a page misread as empty is a flat account.
fn parse_positions_page(body: &str) -> Result<(Vec<Value>, Option<String>)> {
    let page: Value = serde_json::from_str(body).context("positions page is not JSON")?;
    let page = page
        .as_object()
        .ok_or_else(|| anyhow!("positions page is not an object"))?;
    let rows = match page.get("data") {
        Some(Value::Array(rows)) => rows.clone(),
        Some(Value::Null) => Vec::new(),
        Some(_) => return Err(anyhow!("positions page `data` is not an array")),
        None => return Err(anyhow!("positions page has no `data`")),
    };
    let pagination = page
        .get("pagination")
        .and_then(|p| p.as_object())
        .ok_or_else(|| anyhow!("positions page has no `pagination` object"))?;
    let next_cursor = match pagination.get("next_cursor") {
        Some(Value::String(cursor)) => Some(cursor.clone()),
        Some(Value::Null) => None,
        Some(_) => {
            return Err(anyhow!(
                "positions page `pagination.next_cursor` is not a string"
            ))
        }
        None => return Err(anyhow!("positions page has no `pagination.next_cursor`")),
    };
    Ok((rows, next_cursor))
}

/// Available pUSD collateral in the Polymarket deposit wallet, in whole pUSD, from the CLOB's
/// L2-authed GET /balance-allowance. `signature_type=3` matches the deposit-wallet flow every
/// order uses, so the CLOB reports the funder wallet the credentials are bound to — the same
/// pot the entry BUYs spend from. The wire `balance` is a 6-decimals fixed-point integer
/// (pUSD, like USDC before it). The HMAC signs the bare path, query string excluded, same as
/// the official clients.
pub async fn get_poly_balance(trader: &crate::trade::PolyTrader) -> Result<f64> {
    // Longer and more patient than the market-read retry: a balance fetch is off the quoting path,
    // and riding out a minutes-long CLOB or RPC wobble beats reverting to the stale cached value.
    const MAX_ATTEMPTS: u32 = 5;
    const RETRY_DELAY_SEC: u64 = 15;

    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match get_poly_balance_once(trader).await {
            Ok(balance) => return Ok(balance),
            Err(e) => {
                log_event(
                    "Balances",
                    &format!("get_poly_balance attempt {attempt}/{MAX_ATTEMPTS} failed: {e:#}"),
                );
                last_err = Some(e);
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SEC)).await;
                } else {
                    let msg = format!(
                        "get_poly_balance : max attempts ({MAX_ATTEMPTS}) reached, giving up: {:#}",
                        last_err.as_ref().unwrap()
                    );
                    println!("[{}] : {msg}", get_timestamp_ist());
                    log_event("Balances", &msg);
                }
            }
        }
    }
    Err(last_err.unwrap())
}

async fn get_poly_balance_once(trader: &crate::trade::PolyTrader) -> Result<f64> {
    let path = "/balance-allowance";
    let response = trader
        .authed_get(&format!("{path}?asset_type=COLLATERAL&signature_type=3"))
        .await?;
    let text = response.body;
    if !(200..300).contains(&response.status) {
        return Err(anyhow!("GET {path} failed: HTTP {}: {text}", response.status));
    }

    let v: Value = serde_json::from_str(&text)?;
    let base_units = match v.get("balance") {
        Some(Value::String(s)) => s
            .parse::<f64>()
            .with_context(|| format!("balance not a number: {text}"))?,
        Some(Value::Number(n)) => n.as_f64().ok_or_else(|| anyhow!("balance not an f64: {text}"))?,
        _ => return Err(anyhow!("balance missing in response: {text}")),
    };
    Ok(base_units / 1e6)
}

/// Polymarket account value: pUSD collateral plus the mark-to-market value of every held share.
///
/// Cash is [`get_poly_balance`] verbatim — the same number sizing spends from, retried on its own
/// budget. The share half is [`get_poly_marked_positions`].
///
/// **Fail-closed:** if any held position cannot be priced this returns `Err` rather than a partial
/// sum. A silently short total reads as a drawdown, and the caller of this function stops a live
/// bot on a drawdown.
pub async fn get_poly_portfolio(trader: &crate::trade::PolyTrader) -> Result<PortfolioValue> {
    let cash = get_poly_balance(trader).await?;
    let positions = marked_total(&get_poly_marked_positions(trader).await?);
    Ok(PortfolioValue { cash, positions })
}

/// Every Polymarket position with the venue's own mark.
///
/// The data API publishes `current_value` (= `current_size × current_price`) on every row, so no
/// book has to be walked: `current_price` is Polymarket's mark for the token and `current_value` is
/// that mark applied to the exact size held, including its rounding. Verified on v1 2026-09-03
/// against the live `POLY_FUNDER` wallet (six rows, the mark present and `value == size × price` to
/// the cent on all six), and again on v2 2026-11-03 (66 rows, the same identity on every one). A
/// resolved loser reports `current_value: 0` with `redeemable: true`, which is a correct valuation
/// and not a missing mark; only an **absent** field is an error.
///
/// Retried on the balance budget rather than the market-read one, for the same reason: this is off
/// the quoting path, and riding out a minutes-long data-API wobble beats failing a breaker check.
pub async fn get_poly_marked_positions(
    trader: &crate::trade::PolyTrader,
) -> Result<Vec<MarkedPosition>> {
    const MAX_ATTEMPTS: u32 = 5;
    const RETRY_DELAY_SEC: u64 = 15;

    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match get_poly_marked_positions_once(trader).await {
            Ok(positions) => return Ok(positions),
            Err(e) => {
                log_event(
                    "Balances",
                    &format!(
                        "get_poly_marked_positions attempt {attempt}/{MAX_ATTEMPTS} failed: {e:#}"
                    ),
                );
                last_err = Some(e);
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SEC)).await;
                } else {
                    let msg = format!(
                        "get_poly_marked_positions : max attempts ({MAX_ATTEMPTS}) reached, giving up: {:#}",
                        last_err.as_ref().unwrap()
                    );
                    println!("[{}] : {msg}", get_timestamp_ist());
                    log_event("Balances", &msg);
                }
            }
        }
    }
    Err(last_err.unwrap())
}

async fn get_poly_marked_positions_once(
    trader: &crate::trade::PolyTrader,
) -> Result<Vec<MarkedPosition>> {
    fetch_poly_position_rows(trader)
        .await?
        .iter()
        .map(parse_marked_position)
        .collect()
}

/// Split out from [`get_poly_marked_positions`] so the wire shape can be tested against captured
/// JSON. A row missing `current_value` or `current_price` is an `Err` — never a zero.
fn parse_marked_position(position: &Value) -> Result<MarkedPosition> {
    let token_id = position
        .get("token_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("position missing token_id"))?;
    let shares = position
        .get("current_size")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow!("position {token_id} missing current_size"))?;
    let avg_price = position
        .get("avg_price")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow!("position {token_id} missing avg_price"))?;
    let mark = position
        .get("current_price")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow!("position {token_id} missing current_price — cannot be marked"))?;
    let value = position
        .get("current_value")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow!("position {token_id} missing current_value — cannot be marked"))?;
    Ok(MarkedPosition {
        id: token_id.to_string(),
        shares,
        avg_price,
        mark,
        value,
        source: MarkSource::Venue,
    })
}

/// Open orders resting on the Polymarket CLOB for the account the L2 credentials are bound to,
/// as raw JSON from GET /data/orders.
pub async fn get_poly_open_orders(trader: &crate::trade::PolyTrader) -> Result<Value> {
    let path = "/data/orders";
    let response = trader.authed_get(path).await?;
    let text = response.body;
    if !(200..300).contains(&response.status) {
        return Err(anyhow!("GET {path} failed: HTTP {}: {text}", response.status));
    }

    Ok(serde_json::from_str(&text)?)
}

/// Read a numeric field that Polymarket may deliver either as a quoted decimal string or as a
/// bare JSON number. Same tolerance `json_f64` uses in `src/main_op.rs`: every size on
/// /data/trades is quoted today, and a schema change to bare numbers must not read as absent.
fn json_f64(row: &Value, key: &str) -> Option<f64> {
    match row.get(key)? {
        Value::String(s) => s.parse().ok(),
        other => other.as_f64(),
    }
}

/// Lifetime shares of `order_id` that have traded, from the L2-authed CLOB GET /data/trades. The
/// Polymarket analogue of Kalshi's `order_fills` — the authority for what TRADED, as opposed to
/// [`get_poly_open_orders`], which only reports what is still RESTING. A filled order and a
/// cancelled one both vanish from /data/orders identically; only this call tells them apart.
///
/// The endpoint has no order-id query parameter — an unknown parameter is silently ignored, not
/// rejected, so `?id=` returns the whole unfiltered feed — hence the request carries no order id
/// at all and the filtering is client-side, in [`parse_poly_order_trades`].
///
/// Reads ONE page and does not follow `next_cursor`. That is an assumption, not a guarantee: the
/// confirm path only ever asks about an order that traded seconds ago, and the feed is
/// newest-first, so it is on page one. An order asked about long after the fact could page off.
///
/// An order with no trades is `Ok(0.0)` — a genuine cancel is a real answer, not a failure. A
/// transport failure, a non-2xx, or a body that is not the expected `{"data": [...]}` envelope is
/// `Err`: the call site turns `Err` into `RestFill::Unknown`, and answering `Ok(0.0)` when we
/// could not ask would report a live fill as a cancel.
pub async fn get_poly_order_trades(
    trader: &crate::trade::PolyTrader,
    order_id: &str,
) -> Result<f64> {
    let path = "/data/trades";
    let response = trader.authed_get(path).await?;
    let text = response.body;
    if !(200..300).contains(&response.status) {
        return Err(anyhow!("GET {path} failed: HTTP {}: {text}", response.status));
    }

    let body: Value = serde_json::from_str(&text)?;
    // Unlike /positions, this endpoint answers with an object wrapping a `data` array. A body
    // without one is a schema we do not understand, and must not be summed to a confident zero.
    if !body.get("data").is_some_and(Value::is_array) {
        return Err(anyhow!("GET {path}: response carries no `data` array: {text}"));
    }

    Ok(parse_poly_order_trades(&body, order_id))
}

/// Sum OUR maker-side matched size for `order_id` out of a GET /data/trades body. Pure, so it can
/// be tested against the JSON captured from the live account in
/// `sample-resp/sample_response_poly_trades.jsonc`.
///
/// The path is `$.data[*].maker_orders[*].matched_amount` where the sibling `order_id` equals
/// ours. Two rules the shape of this feed makes load-bearing:
///
/// - **Key on `order_id`, never on `maker_address`.** One address can appear twice inside a
///   single `maker_orders[]` array under two different order ids (the capture has such a row,
///   34.87 and 11.371898), and this bot rests several asks on the same token, so summing by
///   address books an unrelated order's size into this one.
/// - **Sum across `data[]` rows as well as within one `maker_orders[]` array.** One resting ask
///   crossed by several takers produces one row per taker, each carrying our order id once;
///   stopping at the first match under-counts a real fill.
///
/// Every top-level field is ignored. `size`, `price`, `side`, `outcome` and `asset_id` describe
/// the TAKER, and `maker_address` holds the taker's address despite its name — it equalled our
/// own funder on 62 of the 63 captured rows, because we were the taker on those. `trader_side`
/// describes our role in the row, not the counterparty's. None of them are safe filters.
///
/// `status` is deliberately not consulted either. Every captured row reads `"CONFIRMED"` because
/// the capture is a day old; a trade seconds old is expected to sit in an earlier state, and
/// filtering it out would answer 0.0 in exactly the window the confirm path cares about — turning
/// a live fill into a cancel.
fn parse_poly_order_trades(body: &Value, order_id: &str) -> f64 {
    let Some(rows) = body.get("data").and_then(Value::as_array) else {
        return 0.0;
    };

    let mut traded = 0.0;
    for row in rows {
        let Some(makers) = row.get("maker_orders").and_then(Value::as_array) else {
            continue;
        };
        for maker in makers {
            if maker.get("order_id").and_then(Value::as_str) == Some(order_id) {
                traded += json_f64(maker, "matched_amount").unwrap_or(0.0);
            }
        }
    }
    traded
}

/// Cancel a resting CLOB order by its id (the `0x…` hash from the order ack or
/// [`get_poly_open_orders`]): L2-authed DELETE /order. Returns the venue's raw JSON
/// (`{"canceled": [...], "not_canceled": {...}}`).
pub async fn cancel_poly_order(
    trader: &crate::trade::PolyTrader,
    order_id: &str,
) -> Result<Value> {
    let path = "/order";
    let body = serde_json::json!({ "orderID": order_id }).to_string();
    let response = trader.authed_delete(path, body).await?;
    let text = response.body;
    if !(200..300).contains(&response.status) {
        return Err(anyhow!("DELETE {path} failed: HTTP {}: {text}", response.status));
    }

    Ok(serde_json::from_str(&text)?)
}

async fn parse_market_detail(
    value: Value,
) -> Result<(Vec<PolymarketMarketDetails>, Vec<MarketSkip>)> {
    let markets = value
        .get("markets")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("markets array missing"))?;

    let outer_id = value
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("outer id missing"))?;

    let outer_slug = value
        .get("slug")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("outer slug missing"))?;

    let is_single_market = markets.len() == 1;

    let mut result = Vec::new();
    let mut skipped: Vec<MarketSkip> = Vec::new();

    for market in markets {
        // What to call this outcome in a skip line. `groupItemTitle` is the outcome name on a
        // multi-market event and is absent on a binary one, where the event slug is the only name
        // there is — `question` is stripped by `pick_fields` long before this runs, and `slug`,
        // kept for `url_slug`, is the market's address, not its name.
        let display_name = market
            .get("groupItemTitle")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| outer_slug.to_string());

        // A live event can still nest dead outcomes — long shots that were closed, de-listed, or
        // had their book switched off while the event itself runs on. Gamma keeps returning them
        // and the CLOB still knows their token ids, but the WS never sends a `book` frame for a
        // token with no order book: the subscription is accepted and then silently ignored. Those
        // tokens sit in the snapshot watchdog's `pending` set forever and tear down the whole
        // 100-token shard on every reconnect, so they must never reach the universe.
        //
        // Absence is read as permissive: `closed`/`archived` default false, `active`/
        // `acceptingOrders` default true, matching Gamma's own omit-when-unset behaviour.
        let flag = |key: &str, default: bool| {
            market.get(key).and_then(Value::as_bool).unwrap_or(default)
        };
        if flag("closed", false)
            || flag("archived", false)
            || !flag("active", true)
            || !flag("acceptingOrders", true)
        {
            // Name the flag that did it rather than the whole disjunction: `closed` on a live event
            // is a resolved long shot, `acceptingOrders: false` is the venue switching a book off,
            // and those are different things to see in a startup report.
            let why = [
                ("closed", true, "closed"),
                ("archived", true, "archived"),
                ("active", false, "active: false"),
                ("acceptingOrders", false, "acceptingOrders: false"),
            ]
            .into_iter()
            .filter(|&(key, tripped_when_true, _)| flag(key, !tripped_when_true) == tripped_when_true)
            .map(|(_, _, label)| label)
            .collect::<Vec<_>>()
            .join(", ");
            skipped.push(MarketSkip::new(&display_name, format!("untradeable ({why})")));
            continue;
        }

        let outcome_prices_str = market
            .get("outcomePrices")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let outcome_prices: Vec<String> =
            serde_json::from_str(outcome_prices_str).unwrap_or_default();

        if outcome_prices.len() >= 2 {
            let yes_price = outcome_prices[0].as_str();
            let no_price = outcome_prices[1].as_str();

            if (yes_price == "0" && no_price == "1")
                || (yes_price == "1" && no_price == "0")
            {
                skipped.push(MarketSkip::new(
                    &display_name,
                    format!("already resolved (outcomePrices {yes_price}/{no_price})"),
                ));
                continue;
            }
        }

        let market_id = if is_single_market {
            outer_id.parse::<u64>().map_err(|_| anyhow!("Failed to parse outer_id as u64"))?
        } else {
            market
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("market id missing"))?
                .parse::<u64>()
                .map_err(|_| anyhow!("Failed to parse market id as u64"))?
        };

        let market_slug = if is_single_market {
            outer_slug.to_string()
        } else {
            match market.get("groupItemTitle").and_then(|v| v.as_str()) {
                Some(title) => title.to_string(),
                // TEAM_LINKS_SPEC TL§3.3: a sports event's main market (its moneyline) has no
                // `groupItemTitle`; its `question` names it. With neither, this one market is skipped
                // and the rest of the event still parses. Before, the whole event failed.
                None => match market
                    .get("question")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|q| !q.is_empty())
                {
                    Some(question) => question.to_string(),
                    None => {
                        skipped.push(MarketSkip::new(
                            &display_name,
                            "no groupItemTitle and no question to name it by".to_string(),
                        ));
                        continue;
                    }
                },
            }
        };

        // The market's own address slug, for links that open the market itself. Not
        // `market_slug`, which is the outcome's name here. None on a one-market event, whose event
        // page already is the market's.
        let url_slug = if is_single_market {
            None
        } else {
            market.get("slug").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string)
        };

        let clob_token_ids_str = market
            .get("clobTokenIds")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("clobTokenIds missing"))?;

        let clob_token_ids: Vec<String> = serde_json::from_str(clob_token_ids_str)?;

        if clob_token_ids.len() < 2 {
            return Err(anyhow!("clobTokenIds must contain at least 2 elements"));
        }

        // TEAM_LINKS_SPEC TL§3.2: the outcome names, in `clobTokenIds` order. Read 2026-11-08 on
        // `nfl-cin-atl-2026-11-08`: outcomes ["Bengals", "Falcons"], outcomePrices ["0.565", "0.435"],
        // and the market's bestBid/bestAsk 0.56/0.57 are token [0]'s, so [0] is the first-named outcome.
        // Only a clean pair of two names over two tokens is kept; anything else is `None` and the market
        // parses exactly as it did before (TL-S2).
        let outcome_labels = market
            .get("outcomes")
            .and_then(Value::as_str)
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .filter(|o| o.len() == 2 && clob_token_ids.len() == 2)
            .map(|o| [o[0].trim().to_string(), o[1].trim().to_string()])
            .filter(|[yes, no]| !yes.is_empty() && !no.is_empty());

        let tick_size = market
            .get("orderPriceMinTickSize")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| anyhow!("orderPriceMinTickSize missing or not a float"))?;

        // Per-market minimum order size in shares. Unlike the tick size this one defaults rather
        // than failing the event: Gamma has carried `orderMinSize` on every live market sampled,
        // but a market that omits it is still tradeable at the conventional 5-share floor.
        let min_order_size = market
            .get("orderMinSize")
            .and_then(|v| v.as_f64())
            .filter(|s| *s > 0.0)
            .unwrap_or(DEFAULT_MIN_ORDER_SIZE);

        // Per-market scheduled resolution time. Defaults to 0 (rather than failing the whole event)
        // if `endDate` is absent/unparseable — it's metadata, not on the trading path.
        let resolution_time = market
            .get("endDate")
            .and_then(|v| v.as_str())
            .and_then(iso8601_to_epoch)
            .unwrap_or(0);

        // Neg-risk markets settle through a different exchange contract, which changes the order
        // signature — the trader must know this per market. Absent field = normal market.
        let neg_risk = market
            .get("negRisk")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Taker fees differ per market category, so the edge math must use this market's own
        // schedule. Fees disabled = fee-free market; fees enabled without a parseable schedule
        // also degrades to fee-free, but loudly — a silent zero here overstates every edge.
        let fee_schedule = if market
            .get("feesEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let parsed = market.get("feeSchedule").and_then(|fs| {
                Some(PolymarketFeeSchedule {
                    rate: fs.get("rate")?.as_f64()?,
                    exponent: fs.get("exponent").and_then(|v| v.as_f64()).unwrap_or(1.0),
                    taker_only: fs.get("takerOnly").and_then(|v| v.as_bool()).unwrap_or(true),
                    rebate_rate: fs.get("rebateRate").and_then(|v| v.as_f64()).unwrap_or(0.0),
                })
            });
            if parsed.is_none() {
                eprintln!(
                    "[{}] : [poly] {market_slug}: feesEnabled but feeSchedule is missing/unparseable — \
                     edge math will treat this market as FEE-FREE",
                    get_timestamp_ist()
                );
            }
            parsed
        } else {
            None
        };

        result.push(PolymarketMarketDetails {
            market_id,
            market_slug,
            url_slug,
            resolution_time,
            neg_risk,
            yes_token_id: clob_token_ids[0].clone(),
            no_token_id: clob_token_ids[1].clone(),
            outcome_labels,
            yes_bids: Arc::new(Mutex::new(Vec::new())),
            yes_asks: Arc::new(Mutex::new(Vec::new())),
            no_bids: Arc::new(Mutex::new(Vec::new())),
            no_asks: Arc::new(Mutex::new(Vec::new())),
            tick_size: Arc::new(Mutex::new(tick_size)),
            min_order_size,
            fee_schedule,
        });
    }

    Ok((result, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The live v1 capture of 2026-09-03 (`GET https://data-api.polymarket.com/positions?...
    /// &user=POLY_FUNDER`), moved to the Data API v2 shape: the `data`/`pagination` envelope and
    /// snake_case keys. **Derived from the published v2 contract (the migration guide and
    /// `/v2/openapi.json`, read 2026-11-03), not a live v2 capture** — re-capture one real page
    /// after the deploy (POLY_DATA_V2_SPEC.md §6). Trimmed to the fields the mark reads plus a
    /// couple that prove the rest is ignored. Two rows: an open position, and a resolved loser
    /// reporting `current_value: 0` with `redeemable: true`.
    const POSITIONS_JSON: &str = r#"{
      "data": [
        {"token_id":"21294592205022969346730955103773391901993330222644504059576935265667917187903",
         "current_size":40.0,"avg_price":0.1549,"current_value":4.42,"current_price":0.1105,
         "redeemable":false,"title":"Will no Fed rate cuts happen in 2026?"},
        {"token_id":"62429084067171308316390058541012087925567325563248051217584518806867987132248",
         "current_size":46.2418,"avg_price":0.2586,"current_value":0,"current_price":0,
         "redeemable":true,"title":"Will LeBron James play for the Miami Heat in 2026-27?"}
      ],
      "pagination": {"next_cursor": null}
    }"#;

    fn rows(json: &str) -> Vec<Value> {
        parse_positions_page(json).unwrap().0
    }

    /// A v2 `/positions` body with these rows and this `next_cursor`.
    fn page(rows: Value, next_cursor: Option<&str>) -> String {
        json!({"data": rows, "pagination": {"next_cursor": next_cursor}}).to_string()
    }

    /// Run the cursor walk over canned bodies, served in order, and hand back what it returned
    /// along with every path it asked for. Running out of bodies is an `Err`, not a panic, so a
    /// walk that reads past the end shows up as a failed assertion.
    async fn walk(bodies: Vec<String>) -> (Result<Vec<Value>>, Vec<String>) {
        let mut paths = Vec::new();
        let mut bodies = bodies.into_iter();
        let result = walk_position_pages(|path| {
            paths.push(path);
            let body = bodies.next();
            async move { body.ok_or_else(|| anyhow!("the walk read past the last canned page")) }
        })
        .await;
        (result, paths)
    }

    fn token_ids(rows: &[Value]) -> Vec<&str> {
        rows.iter()
            .map(|r| r["token_id"].as_str().unwrap())
            .collect()
    }

    /// The live shape marks straight off the venue's own `current_value`, and a resolved loser's
    /// zero is a valuation, not a missing mark.
    #[test]
    fn real_positions_mark_at_the_venue_value() {
        let marked: Vec<MarkedPosition> =
            rows(POSITIONS_JSON).iter().map(|r| parse_marked_position(r).unwrap()).collect();
        assert_eq!(marked.len(), 2);
        assert_eq!(marked[0].shares, 40.0);
        assert_eq!(marked[0].avg_price, 0.1549);
        assert_eq!(marked[0].mark, 0.1105);
        assert_eq!(marked[0].value, 4.42);
        assert_eq!(marked[0].source, MarkSource::Venue);
        // `current_value` is the venue's own `current_size × current_price`, to the cent.
        assert!((marked[0].value - marked[0].shares * marked[0].mark).abs() < 1e-6);
        assert_eq!(marked[1].value, 0.0);
        assert_eq!(marked_total(&marked), 4.42);
    }

    /// A flat account is an empty array, not an error, and sums to zero.
    #[test]
    fn a_flat_account_marks_to_zero() {
        assert_eq!(marked_total(&[]), 0.0);
        assert!(rows(&page(json!([]), None)).is_empty());
    }

    /// **Fail-closed.** A row with no `current_value` is an `Err`, never a zero folded into the
    /// sum: a silently short total reads as a drawdown, and the breaker stops a live bot on one.
    #[test]
    fn a_row_missing_its_mark_is_an_error_not_a_short_total() {
        let no_value =
            json!({"token_id":"111","current_size":40.0,"avg_price":0.1549,"current_price":0.1105});
        let err = parse_marked_position(&no_value).unwrap_err().to_string();
        assert!(err.contains("current_value"), "{err}");

        let no_price =
            json!({"token_id":"111","current_size":40.0,"avg_price":0.1549,"current_value":4.42});
        let err = parse_marked_position(&no_price).unwrap_err().to_string();
        assert!(err.contains("current_price"), "{err}");
    }

    /// PV2-T1. The rows and the cursor both come out of the envelope.
    #[test]
    fn positions_page_parses_the_v2_envelope() {
        let body = page(json!([{"token_id":"1"}, {"token_id":"2"}]), Some("abc"));
        let (rows, next_cursor) = parse_positions_page(&body).unwrap();
        assert_eq!(token_ids(&rows), ["1", "2"]);
        assert_eq!(next_cursor.as_deref(), Some("abc"));

        let (rows, next_cursor) = parse_positions_page(POSITIONS_JSON).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(next_cursor, None);
    }

    /// PV2-T2. `data: null` is the v2 contract's documented miss: no positions, not an error.
    #[test]
    fn a_null_data_page_is_no_positions_not_an_error() {
        let (rows, next_cursor) = parse_positions_page(&page(Value::Null, None)).unwrap();
        assert!(rows.is_empty());
        assert_eq!(next_cursor, None);
    }

    /// PV2-T3. A page that does not say whether it is the last is an `Err`, never an end: read as
    /// one, it would cut the list short. So is a page with no `data`, and the bare v1 array.
    #[test]
    fn a_page_with_no_pagination_object_is_an_error() {
        let err = parse_positions_page(r#"{"data":[{"token_id":"1"}]}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("pagination"), "{err}");

        let err = parse_positions_page(r#"{"data":[],"pagination":{}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("next_cursor"), "{err}");

        let err = parse_positions_page(r#"{"pagination":{"next_cursor":null}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("data"), "{err}");

        assert!(parse_positions_page(r#"[{"token_id":"1"}]"#).is_err());
    }

    /// PV2-T4. The cursor is opaque and may hold `+`, `/` and `=`; sent raw, `+` reads as a space.
    /// And it carries none of the query, so every other parameter is repeated unchanged.
    #[tokio::test]
    async fn the_cursor_is_percent_encoded_and_every_filter_is_repeated() {
        let first = positions_path(None, None);
        assert_eq!(
            first,
            "/positions?filter_type=TOKENS&filter_amount=1&sort_by=TOKENS&sort_direction=DESC&limit=1000"
        );
        assert_eq!(
            positions_path(None, Some("a+b/c=")),
            format!("{first}&cursor=a%2Bb%2Fc%3D")
        );

        // The same through the walk: page two is page one's request plus the encoded cursor.
        let (result, paths) = walk(vec![
            page(json!([{"token_id":"1"}]), Some("a+b/c=")),
            page(json!([{"token_id":"2"}]), None),
        ])
        .await;
        assert_eq!(token_ids(&result.unwrap()), ["1", "2"]);
        assert_eq!(
            paths,
            [first.clone(), format!("{first}&cursor=a%2Bb%2Fc%3D")]
        );
    }

    /// PV2-T5. v2 answers any `offset`, even `0`, with HTTP 400 — and none of v1's spellings either.
    #[tokio::test]
    async fn the_first_request_never_carries_offset() {
        for status in POSITIONS_PASSES {
            for path in [
                positions_path(status, None),
                positions_path(status, Some("abc")),
            ] {
                assert!(!path.contains("offset"), "{path}");
                assert!(!path.contains("sizeThreshold"), "{path}");
                assert!(!path.contains("sortBy"), "{path}");
            }
        }
        let (_, paths) = walk(vec![page(json!([]), None)]).await;
        assert_eq!(paths.len(), 1);
        assert!(!paths[0].contains("offset"), "{}", paths[0]);
    }

    /// PV2-T6. A cursor handed back twice would spin the walk forever; it is an `Err`, and the
    /// walk stops asking.
    #[tokio::test]
    async fn a_repeated_cursor_is_an_error() {
        let (result, paths) = walk(vec![
            page(json!([{"token_id":"1"}]), Some("abc")),
            page(json!([{"token_id":"2"}]), Some("abc")),
            page(json!([]), None),
        ])
        .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("same cursor twice"), "{err}");
        assert_eq!(paths.len(), 2);
    }

    /// PV2-T7. Only a `null` cursor ends the walk. A one-row page and an empty page, each with a
    /// cursor, are read on; and a token served again on a later page is kept once.
    #[tokio::test]
    async fn the_walk_stops_on_a_null_cursor_not_a_short_page() {
        let (result, paths) = walk(vec![
            page(json!([{"token_id":"1"}]), Some("abc")),
            page(json!([]), Some("def")),
            page(json!([{"token_id":"2"}, {"token_id":"1"}]), None),
        ])
        .await;
        assert_eq!(token_ids(&result.unwrap()), ["1", "2"]);
        assert_eq!(paths.len(), 3);
    }

    /// PV2-T8. A walk that never sees a `null` cursor is stopped at the bound with an `Err`. A
    /// silent stop there would hand back a cut-short list, which reads as a flatter account than
    /// the real one.
    #[tokio::test]
    async fn the_page_bound_is_an_error_not_a_truncation() {
        let mut fetched = 0usize;
        let result = walk_position_pages(|_path| {
            fetched += 1;
            let body = page(
                json!([{"token_id": fetched.to_string()}]),
                Some(&format!("c{fetched}")),
            );
            async move { Ok(body) }
        })
        .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
        assert_eq!(fetched, POSITIONS_MAX_PAGES);
    }

    /// A market object shaped like the live world-cup-winner capture (2026-07-18, feeType
    /// sports_fees_v2), trimmed to the fields the pipeline touches plus one stray field to
    /// prove the filter still drops what it should.
    fn raw_sports_market() -> Value {
        json!({
            "id": "553900",
            "groupItemTitle": "Spain",
            "clobTokenIds": "[\"111\", \"222\"]",
            "orderPriceMinTickSize": 0.001,
            "outcomePrices": "[\"0.5915\", \"0.4085\"]",
            "outcomes": "[\"Yes\", \"No\"]",
            "negRisk": true,
            "endDate": "2026-07-19T00:00:00Z",
            "feesEnabled": true,
            "feeType": "sports_fees_v2",
            "feeSchedule": { "exponent": 1, "rate": 0.05, "rebateRate": 0.15, "takerOnly": true },
            "volume": "1234.5"
        })
    }

    /// Regression: `feeSchedule` must survive the event field filter. It was missing from the
    /// pick list until 2026-07-18, so every market parsed with `fee_schedule: None` and the
    /// arbitrage math priced Polymarket's taker fee at zero on fee-charging markets.
    #[tokio::test]
    async fn fee_schedule_survives_field_filter() {
        let picked = pick_fields(&raw_sports_market(), &MARKET_FIELDS);
        assert!(picked.get("feeSchedule").is_some_and(|v| !v.is_null()));
        assert!(picked.get("volume").is_none(), "unlisted fields must still be dropped");

        let event = json!({ "id": "27799", "slug": "world-cup-winner", "markets": [picked] });
        let (details, skipped) = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1);
        assert!(skipped.is_empty());
        let fs = details[0]
            .fee_schedule
            .expect("fees-enabled market must carry its schedule through the filter");
        assert_eq!(fs.rate, 0.05);
        assert_eq!(fs.exponent, 1.0);
        // rate × p(1−p) × shares, the on-chain-verified taker fee, on the live Spain ask.
        let fee = fs.taker_fee(0.5915, 1.0);
        assert!((fee - 0.05 * 0.5915 * 0.4085).abs() < 1e-12, "got {fee}");
    }

    /// Links Stage 2: a market's own Gamma `slug` must survive the field filter, and becomes
    /// `url_slug` on a multi-market event only. `market_slug` is untouched: the outcome's name
    /// there, the event slug on a one-market event.
    #[tokio::test]
    async fn ps1_url_slug_survives_field_filter() {
        let spain_slug = "will-spain-win-the-2026-fifa-world-cup-963";
        let brazil_slug = "will-brazil-win-the-2026-fifa-world-cup-412";

        let mut spain = raw_sports_market();
        spain["slug"] = json!(spain_slug);
        let spain = pick_fields(&spain, &MARKET_FIELDS);
        assert_eq!(spain.get("slug"), Some(&json!(spain_slug)), "slug must survive the filter");

        let mut brazil = raw_sports_market();
        brazil["id"] = json!("553901");
        brazil["groupItemTitle"] = json!("Brazil");
        brazil["clobTokenIds"] = json!("[\"333\", \"444\"]");
        brazil["slug"] = json!(brazil_slug);
        let brazil = pick_fields(&brazil, &MARKET_FIELDS);

        // Two markets: each keeps its own slug, and each `market_slug` is still its outcome name.
        let event =
            json!({ "id": "27799", "slug": "world-cup-winner", "markets": [spain, brazil] });
        let (details, skipped) = parse_market_detail(event).await.unwrap();
        assert!(skipped.is_empty());
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].market_slug, "Spain");
        assert_eq!(details[0].url_slug.as_deref(), Some(spain_slug));
        assert_eq!(details[1].market_slug, "Brazil");
        assert_eq!(details[1].url_slug.as_deref(), Some(brazil_slug));

        // Spain alone in its event: the event page is the market's, so no `url_slug`, and
        // `market_slug` is the event slug as before.
        let event = json!({ "id": "27799", "slug": "world-cup-winner", "markets": [spain] });
        let (details, _) = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].url_slug, None);
        assert_eq!(details[0].market_slug, "world-cup-winner");

        // Brazil without a `slug` key: it arrives as `null` and gets `None`; Spain keeps hers.
        let mut brazil = raw_sports_market();
        brazil["id"] = json!("553901");
        brazil["groupItemTitle"] = json!("Brazil");
        brazil["clobTokenIds"] = json!("[\"333\", \"444\"]");
        let brazil = pick_fields(&brazil, &MARKET_FIELDS);
        let event =
            json!({ "id": "27799", "slug": "world-cup-winner", "markets": [spain, brazil] });
        let (details, _) = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].url_slug.as_deref(), Some(spain_slug));
        assert_eq!(details[1].url_slug, None);
    }

    /// Fee-free markets (feesEnabled false or absent) still parse with no schedule.
    #[tokio::test]
    async fn fee_free_market_parses_without_schedule() {
        let mut raw = raw_sports_market();
        raw["feesEnabled"] = json!(false);
        let picked = pick_fields(&raw, &MARKET_FIELDS);
        let event = json!({ "id": "1", "slug": "no-fees", "markets": [picked] });
        let (details, _) = parse_market_detail(event).await.unwrap();
        assert!(details[0].fee_schedule.is_none());
    }

    /// Regression: a live event nesting dead outcomes must yield only the tradeable ones. Before
    /// 2026-08-25 the only per-market rejection was the degenerate 0/1 price pair, so closed,
    /// archived, inactive and order-book-off long shots (priced 0.002/0.998, not degenerate)
    /// reached the WS universe — where they never snapshot and reconnect-loop the whole shard.
    #[tokio::test]
    async fn untradeable_outcomes_are_dropped() {
        let dead = |title: &str, key: &str, val: bool| {
            let mut raw = raw_sports_market();
            raw["groupItemTitle"] = json!(title);
            raw["outcomePrices"] = json!("[\"0.002\", \"0.998\"]");
            raw[key] = json!(val);
            pick_fields(&raw, &MARKET_FIELDS)
        };

        let mut alive = raw_sports_market();
        alive["active"] = json!(true);
        alive["acceptingOrders"] = json!(true);
        alive["closed"] = json!(false);
        alive["archived"] = json!(false);

        let event = json!({
            "id": "27799",
            "slug": "us-open-winner",
            "markets": [
                pick_fields(&alive, &MARKET_FIELDS),
                dead("Closed", "closed", true),
                dead("Archived", "archived", true),
                dead("Inactive", "active", false),
                dead("NoOrders", "acceptingOrders", false),
            ],
        });

        let (details, skipped) = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1, "only the tradeable outcome survives");
        assert_eq!(details[0].market_slug, "Spain");

        // Every drop is named and reasoned, in the order the event listed them — this is what the
        // startup report prints, and a silent count could not tell "Closed" from "NoOrders".
        let named: Vec<(&str, &str)> =
            skipped.iter().map(|s| (s.name.as_str(), s.reason.as_str())).collect();
        assert_eq!(
            named,
            vec![
                ("Closed", "untradeable (closed)"),
                ("Archived", "untradeable (archived)"),
                ("Inactive", "untradeable (active: false)"),
                ("NoOrders", "untradeable (acceptingOrders: false)"),
            ],
        );
    }

    /// An outcome the market has already settled is dropped too, and — unlike before — it is
    /// *reported*. This branch never touched the old `untradeable` counter, so a resolved long shot
    /// left the universe with nothing printed anywhere.
    #[tokio::test]
    async fn resolved_outcomes_are_reported_not_silently_dropped() {
        let mut resolved = raw_sports_market();
        resolved["groupItemTitle"] = json!("Brazil");
        resolved["outcomePrices"] = json!("[\"0\", \"1\"]");

        let event = json!({
            "id": "27799",
            "slug": "world-cup-winner",
            "markets": [
                pick_fields(&raw_sports_market(), &MARKET_FIELDS),
                pick_fields(&resolved, &MARKET_FIELDS),
            ],
        });

        let (details, skipped) = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "Brazil");
        assert!(skipped[0].reason.contains("already resolved"), "{}", skipped[0].reason);
    }

    /// Markets that omit the tradeability flags are kept. `pick_fields` inserts every listed key,
    /// so an omitted flag arrives as JSON `null`, not as a missing key — the permissive defaults
    /// in the filter are what stop a null from emptying the universe.
    #[tokio::test]
    async fn absent_tradeability_flags_are_permissive() {
        let picked = pick_fields(&raw_sports_market(), &MARKET_FIELDS);
        assert!(picked["acceptingOrders"].is_null(), "fixture omits the flag");
        let event = json!({ "id": "1", "slug": "no-flags", "markets": [picked] });
        assert_eq!(parse_market_detail(event).await.unwrap().0.len(), 1);
    }

    /// TEAM_LINKS_SPEC TL§3.5: the game market, trimmed from the 2026-11-08 Gamma read of
    /// `nfl-cin-atl-2026-11-08`, with made-up token ids. It has no `groupItemTitle`.
    fn raw_game_market() -> Value {
        json!({
            "id": "900",
            "question": "Bengals vs. Falcons",
            "slug": "nfl-cin-atl-2026-11-08",
            "outcomes": "[\"Bengals\", \"Falcons\"]",
            "outcomePrices": "[\"0.565\", \"0.435\"]",
            "clobTokenIds": "[\"111\", \"222\"]",
            "active": true,
            "closed": false,
            "acceptingOrders": true,
            "orderPriceMinTickSize": 0.01,
            "endDate": "2026-11-08T14:30:00Z",
            "negRisk": false,
            "feesEnabled": false
        })
    }

    /// The spread beside it, as Gamma lists it.
    fn raw_spread_market() -> Value {
        json!({
            "id": "901",
            "question": "Spread: Bengals (-1.5)",
            "groupItemTitle": "Spread -1.5",
            "outcomes": "[\"Bengals\", \"Falcons\"]",
            "outcomePrices": "[\"0.53\", \"0.47\"]",
            "clobTokenIds": "[\"333\", \"444\"]",
            "orderPriceMinTickSize": 0.01,
            "endDate": "2026-11-08T14:30:00Z"
        })
    }

    fn game_event(game: &Value) -> Value {
        json!({
            "id": "77",
            "slug": "nfl-cin-atl-2026-11-08",
            "markets": [
                pick_fields(game, &MARKET_FIELDS),
                pick_fields(&raw_spread_market(), &MARKET_FIELDS)
            ]
        })
    }

    fn labels(a: &str, b: &str) -> Option<[String; 2]> {
        Some([a.to_string(), b.to_string()])
    }

    /// TP1. A sports event's main market has no `groupItemTitle`: its `question` names it, the
    /// event still parses, and both markets carry their outcome names in token order.
    #[tokio::test]
    async fn a_moneyline_without_group_item_title_is_named_by_its_question() {
        let (details, skipped) = parse_market_detail(game_event(&raw_game_market())).await.unwrap();
        assert!(skipped.is_empty(), "{skipped:?}");
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].market_slug, "Bengals vs. Falcons");
        assert_eq!(details[0].yes_token_id, "111");
        assert_eq!(details[0].no_token_id, "222");
        assert_eq!(details[0].outcome_labels, labels("Bengals", "Falcons"));
        assert_eq!(details[1].market_slug, "Spread -1.5");
        assert_eq!(details[1].outcome_labels, labels("Bengals", "Falcons"));
    }

    /// TP2. An ordinary market carries `["Yes", "No"]`.
    #[tokio::test]
    async fn a_yes_no_market_carries_yes_and_no() {
        let picked = pick_fields(&raw_sports_market(), &MARKET_FIELDS);
        let event = json!({ "id": "27799", "slug": "world-cup-winner", "markets": [picked] });
        let (details, _) = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].outcome_labels, labels("Yes", "No"));
    }

    /// TP3. Labels never skip or refuse a market (TL-S2): without a clean pair of two names over two
    /// tokens the labels are `None` and the market parses as it always did.
    #[tokio::test]
    async fn labels_that_are_not_a_clean_pair_change_nothing() {
        let mut no_outcomes = raw_game_market();
        no_outcomes.as_object_mut().unwrap().remove("outcomes");
        let mut three_names = raw_game_market();
        three_names["outcomes"] = json!("[\"Bengals\", \"Falcons\", \"Tie\"]");
        let mut three_tokens = raw_game_market();
        three_tokens["clobTokenIds"] = json!("[\"111\", \"222\", \"333\"]");

        for game in [no_outcomes, three_names, three_tokens] {
            let (details, skipped) = parse_market_detail(game_event(&game)).await.unwrap();
            assert!(skipped.is_empty(), "{skipped:?}");
            assert_eq!(details.len(), 2);
            assert_eq!(details[0].market_slug, "Bengals vs. Falcons");
            assert_eq!(details[0].yes_token_id, "111");
            assert_eq!(details[0].no_token_id, "222");
            assert_eq!(details[0].outcome_labels, None);
            assert_eq!(details[1].market_slug, "Spread -1.5");
            assert_eq!(details[1].outcome_labels, labels("Bengals", "Falcons"));
        }
    }

    /// TP4. A market with neither `groupItemTitle` nor `question` is skipped with a reason; the
    /// event still parses (before, the whole event failed).
    #[tokio::test]
    async fn a_market_with_no_name_is_skipped_not_fatal() {
        let mut game = raw_game_market();
        game.as_object_mut().unwrap().remove("question");
        let (details, skipped) = parse_market_detail(game_event(&game)).await.unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].market_slug, "Spread -1.5");
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].reason, "no groupItemTitle and no question to name it by");
    }

    /// TP5. `question` must survive the field filter, or the name fallback never sees it.
    #[tokio::test]
    async fn question_survives_field_filter() {
        let picked = pick_fields(&raw_game_market(), &MARKET_FIELDS);
        assert_eq!(picked["question"], json!("Bengals vs. Falcons"));
    }

    // ---------------------------------------------------------------------------------------
    // GET /data/trades parsing. Every row below is copied VERBATIM out of the Stage 0 capture
    // in `sample-resp/sample_response_poly_trades.jsonc` (candidate D, the whole 63-row feed
    // for POLY_FUNDER). Anything constructed rather than captured is labelled as such.
    // ---------------------------------------------------------------------------------------

    /// Ground truth from the capture: this order FILLED, selling 15.88 @ 0.13 on 2026-08-29.
    const FILLED_ID: &str = "0x88bf0895acf136b3e43360b8a754d66ce16a3a8aafb6c842e4ee419604474920";
    /// Ground truth from the capture: both were cancelled by us, nothing traded.
    const CANCELLED_IDS: [&str; 2] = [
        "0xa102ed903de2507dc946c29b7002edf24d0334ff804f4c24604e14534661aa42",
        "0x9348a82b8b5b3d456d6c06e88af5fc46c748f723d7798fd702044056cca4321e",
    ];

    /// The one captured row where we were the MAKER — our filled ask, nested one level down.
    /// Note the top-level `side`/`price`/`outcome` describe the taker: SELL 15.88 @ 0.87 of "No".
    const ROW_OURS_FILLED: &str = r#"{
      "id": "87d21cc9-14b8-4f00-b316-76b60acb7e4a",
      "taker_order_id": "0x4bb32e66b17b35e8f80e8e322475703cefad7d99fa41bb2c2c344a0be5b554f2",
      "market": "0x48d47eede033257e77d676631e6f805d5e1848c0567589030a3a76a862fa1e47",
      "asset_id": "7219288355080279781244318078654828822747329147903096891496889491264445925442",
      "side": "SELL",
      "size": "15.88",
      "fee_rate_bps": "0",
      "price": "0.87",
      "status": "CONFIRMED",
      "match_time": "1787980569",
      "last_update": "1787980577",
      "outcome": "No",
      "bucket_index": 0,
      "owner": "699a8016-2639-997b-e72e-916e57783395",
      "maker_address": "0x1E60D8A80Fa1C49B4E5B3f7b0043ef1bc08FDd19",
      "transaction_hash": "0x49942c4826fbb80c811e70384f32b154979314b05d435f577f420d4178461bd0",
      "maker_orders": [
        {
          "order_id": "0x88bf0895acf136b3e43360b8a754d66ce16a3a8aafb6c842e4ee419604474920",
          "owner": "f13cd2a4-7647-a9ed-eb2e-30a05e27b270",
          "maker_address": "0x9fA39F520b0bfabB81cDe4a0AB06Ec49FbDCCA65",
          "matched_amount": "15.88",
          "price": "0.13",
          "fee_rate_bps": "",
          "asset_id": "4301913150008555036242742378121421425636969753583463858852315615150555927156",
          "outcome": "Yes",
          "side": "SELL"
        }
      ],
      "trader_side": "MAKER"
    }"#;

    /// A captured row where we were the TAKER. `maker_address` at the top level is our own
    /// funder here — that field names the taker, not the maker, which is why it is never a
    /// filter. The maker is a stranger's order id.
    const ROW_OURS_TAKER: &str = r#"{
      "id": "2b4fbc2b-361a-482a-9b0b-a9156e9eaafe",
      "taker_order_id": "0xa44e3dd7618ee84ea9fadb58e7a87fdc69a357890e15efc5d621b57272d03eac",
      "market": "0x48d47eede033257e77d676631e6f805d5e1848c0567589030a3a76a862fa1e47",
      "asset_id": "4301913150008555036242742378121421425636969753583463858852315615150555927156",
      "side": "SELL",
      "size": "4.12",
      "fee_rate_bps": "0",
      "price": "0.13",
      "status": "CONFIRMED",
      "match_time": "1787987314",
      "last_update": "1787987322",
      "outcome": "Yes",
      "bucket_index": 0,
      "owner": "f13cd2a4-7647-a9ed-eb2e-30a05e27b270",
      "maker_address": "0x9fA39F520b0bfabB81cDe4a0AB06Ec49FbDCCA65",
      "transaction_hash": "0x2b8897b292df951667ef3419e304fc557b96ac5366c61af0b93b863a331085f8",
      "maker_orders": [
        {
          "order_id": "0x4bb32e66b17b35e8f80e8e322475703cefad7d99fa41bb2c2c344a0be5b554f2",
          "owner": "699a8016-2639-997b-e72e-916e57783395",
          "maker_address": "0x1E60D8A80Fa1C49B4E5B3f7b0043ef1bc08FDd19",
          "matched_amount": "4.12",
          "price": "0.87",
          "fee_rate_bps": "",
          "asset_id": "7219288355080279781244318078654828822747329147903096891496889491264445925442",
          "outcome": "No",
          "side": "SELL"
        }
      ],
      "trader_side": "TAKER"
    }"#;

    /// The captured row that makes `maker_address` unusable as a key: ONE address
    /// (0xB0C858…) appears twice in `maker_orders`, under two different order ids, for
    /// 34.87 and 11.371898. Summing by address would book 46.241898 for either one.
    const ROW_ONE_ADDRESS_TWO_ORDERS: &str = r#"{
      "id": "18c2d4dd-4fa3-4c03-8cab-d02bb571ef99",
      "taker_order_id": "0x0e205e20f3c102c60f4193d4595738830ea20c95ec1f7d174de9f3c8e934a711",
      "market": "0x0ff22cf1f7f86a8f6a1f46ebff9afc02d2f2f8be82ed1655a079fd2f8b8d450e",
      "asset_id": "62429084067171308316390058541012087925567325563248051217584518806867987132248",
      "side": "BUY",
      "size": "46.241898",
      "fee_rate_bps": "0",
      "price": "0.259",
      "status": "CONFIRMED",
      "match_time": "1784907059",
      "last_update": "1784907068",
      "outcome": "Yes",
      "bucket_index": 0,
      "owner": "f13cd2a4-7647-a9ed-eb2e-30a05e27b270",
      "maker_address": "0x9fA39F520b0bfabB81cDe4a0AB06Ec49FbDCCA65",
      "transaction_hash": "0xdf008d786253ba3504447050d6d78ae3c7a80048800bbfe66a90f4ece97ccf07",
      "maker_orders": [
        {
          "order_id": "0x9630422d76f039fa7dc532e660c27b1ca86df3e2fe0787f197c652ac2a60df2c",
          "owner": "e2d7b3ae-65f1-625a-c786-ef5721cbd6a3",
          "maker_address": "0xB0C85813a7a4428F1139ff91D3118A92C391fE7F",
          "matched_amount": "34.87",
          "price": "0.748",
          "fee_rate_bps": "",
          "asset_id": "89875746697207293701645144951802475381650054489316986030574927315911706093686",
          "outcome": "No",
          "side": "BUY"
        },
        {
          "order_id": "0x4efe67b0a83da0acbcf73c6adef4b3926793a0883dcf81518cd2fd2487c99357",
          "owner": "e2d7b3ae-65f1-625a-c786-ef5721cbd6a3",
          "maker_address": "0xB0C85813a7a4428F1139ff91D3118A92C391fE7F",
          "matched_amount": "11.371898",
          "price": "0.7210000476613491",
          "fee_rate_bps": "",
          "asset_id": "89875746697207293701645144951802475381650054489316986030574927315911706093686",
          "outcome": "No",
          "side": "BUY"
        }
      ],
      "trader_side": "TAKER"
    }"#;

    /// A captured row with FOUR makers behind one taker, top-level `size` 2547.096773. None of
    /// the four is ours; test 6 substitutes our id into one of them.
    const ROW_FOUR_MAKERS: &str = r#"{
      "id": "e5f1dbdc-a065-40bc-ab1d-a8a6ea7c5ca4",
      "taker_order_id": "0x093484a1ca4f8d553bf89e1bf575f1b48e9c18aacdf612ca5a0395130f827b60",
      "market": "0xde72dd287ba34b31dc69066931b7c0074f8267132bf7d6752e2f736429ae98af",
      "asset_id": "74307145292583766935914203363521126434260090816880819164570227844862844637714",
      "side": "BUY",
      "size": "2547.096773",
      "fee_rate_bps": "0",
      "price": "0.011",
      "status": "CONFIRMED",
      "match_time": "1784907056",
      "last_update": "1784907065",
      "outcome": "No",
      "bucket_index": 0,
      "owner": "f13cd2a4-7647-a9ed-eb2e-30a05e27b270",
      "maker_address": "0x9fA39F520b0bfabB81cDe4a0AB06Ec49FbDCCA65",
      "transaction_hash": "0xfda5d3725bf24b970d30083af3248175e3c7987751320127984352b79ca7ef66",
      "maker_orders": [
        {
          "order_id": "0xbd0b02979c2037ab578a3857e8b1cdc50b778f30e936a31ef729f0c3ab5d7603",
          "owner": "0b1c7046-9d74-c26b-a8c5-4552a019efdb",
          "maker_address": "0xc2E5359B204c4296B7e1a07603fe0B657486b3A5",
          "matched_amount": "1740",
          "price": "0.99",
          "fee_rate_bps": "",
          "asset_id": "75083988390277121768686920804826665792234222050775941532408375203298640433019",
          "outcome": "Yes",
          "side": "BUY"
        },
        {
          "order_id": "0x8300b9fcabcbe25d690fc7d52b139950963e9880f69ed941436f81d3edb8cc73",
          "owner": "57f3e0a2-96e2-bfa6-0e67-aa0f5c7190ca",
          "maker_address": "0xA0f21E6d351BAa9185716B5c00C2925Ed9621848",
          "matched_amount": "600",
          "price": "0.99",
          "fee_rate_bps": "",
          "asset_id": "75083988390277121768686920804826665792234222050775941532408375203298640433019",
          "outcome": "Yes",
          "side": "BUY"
        },
        {
          "order_id": "0xa2b46aebd8a22514aaa361f25f385ba3fe82159f4ab6da82cbdd0532c0102d63",
          "owner": "1833aba8-1d38-9a12-10c2-939be18b5122",
          "maker_address": "0xa86F687D7EED8760decdbf3Dd6d7eD59C1E8022C",
          "matched_amount": "200",
          "price": "0.989",
          "fee_rate_bps": "",
          "asset_id": "75083988390277121768686920804826665792234222050775941532408375203298640433019",
          "outcome": "Yes",
          "side": "BUY"
        },
        {
          "order_id": "0x2999da7c4e3168dba6b23fb225ee188d54500c0def4249ee0e3e628e021a8c5d",
          "owner": "9cc0aeaa-71c2-2d71-52f3-d7dce5a4c093",
          "maker_address": "0x661DaF6Af6D884012dd6db73C09D72E8BE224Dc6",
          "matched_amount": "7.096773",
          "price": "0.3099999112272578",
          "fee_rate_bps": "",
          "asset_id": "74307145292583766935914203363521126434260090816880819164570227844862844637714",
          "outcome": "No",
          "side": "SELL"
        }
      ],
      "trader_side": "TAKER"
    }"#;

    fn row(raw: &str) -> Value {
        serde_json::from_str(raw).expect("captured row must parse")
    }

    /// Wrap captured rows in the envelope the endpoint actually answers with — an object with a
    /// `data` array, not the bare array `/positions` returns. Field names, `limit` and
    /// `next_cursor` are copied from the capture.
    fn feed(rows: Vec<Value>) -> Value {
        let count = rows.len();
        json!({ "data": rows, "next_cursor": "LTE=", "limit": 300, "count": count })
    }

    /// Test 1 — the incident. Our filled ask parses to the 15.88 the venue actually matched,
    /// out of a feed that also carries unrelated rows.
    #[test]
    fn filled_order_parses_to_its_maker_side_size() {
        let body = feed(vec![
            row(ROW_OURS_TAKER),
            row(ROW_OURS_FILLED),
            row(ROW_ONE_ADDRESS_TWO_ORDERS),
            row(ROW_FOUR_MAKERS),
        ]);
        let traded = parse_poly_order_trades(&body, FILLED_ID);
        assert!((traded - 15.88).abs() < 1e-9, "got {traded}");
    }

    /// Test 2 — both orders we cancelled parse to 0.0 against that same feed. This is the answer
    /// that means "genuine cancel", and it must not be reachable any other way.
    #[test]
    fn cancelled_orders_parse_to_zero() {
        let body = feed(vec![
            row(ROW_OURS_TAKER),
            row(ROW_OURS_FILLED),
            row(ROW_ONE_ADDRESS_TWO_ORDERS),
            row(ROW_FOUR_MAKERS),
        ]);
        for id in CANCELLED_IDS {
            let traded = parse_poly_order_trades(&body, id);
            assert!(traded.abs() < 1e-9, "{id} got {traded}");
        }
    }

    /// Test 3 — a feed of other orders' trades, including two from our own account as taker,
    /// parses to 0.0 for our order id. The same body proves the address is not the key: asking
    /// for one of the two order ids sharing address 0xB0C858… returns that order's 34.87, not
    /// the address's combined 46.241898.
    #[test]
    fn other_orders_do_not_leak_into_ours() {
        let body = feed(vec![
            row(ROW_OURS_TAKER),
            row(ROW_ONE_ADDRESS_TWO_ORDERS),
            row(ROW_FOUR_MAKERS),
        ]);
        let traded = parse_poly_order_trades(&body, FILLED_ID);
        assert!(traded.abs() < 1e-9, "got {traded}");

        let first = "0x9630422d76f039fa7dc532e660c27b1ca86df3e2fe0787f197c652ac2a60df2c";
        let traded = parse_poly_order_trades(&body, first);
        assert!((traded - 34.87).abs() < 1e-9, "keyed on address, not order id: got {traded}");
    }

    /// Test 4 — CONSTRUCTED, not captured: the endpoint quotes every size it has ever returned.
    /// Derived from the real filled row by unquoting `matched_amount`, so a schema change to
    /// bare numbers reads the same rather than reading as an absent field.
    #[test]
    fn bare_number_size_parses_like_a_quoted_one() {
        let mut r = row(ROW_OURS_FILLED);
        r["maker_orders"][0]["matched_amount"] = json!(15.88);
        let traded = parse_poly_order_trades(&feed(vec![r]), FILLED_ID);
        assert!((traded - 15.88).abs() < 1e-9, "got {traded}");
    }

    /// Test 5 — CONSTRUCTED, not captured: every captured `maker_orders` array is non-empty.
    /// Derived from the real filled row by emptying it. An empty array is not a fill.
    #[test]
    fn empty_maker_orders_parses_to_zero() {
        let mut r = row(ROW_OURS_FILLED);
        r["maker_orders"] = json!([]);
        let traded = parse_poly_order_trades(&feed(vec![r]), FILLED_ID);
        assert!(traded.abs() < 1e-9, "got {traded}");
    }

    /// Test 6 — the case that silently over-books. CONSTRUCTED from the real four-maker row by
    /// substituting our order id into the fourth maker; the capture has genuine 3- and 4-maker
    /// rows, but none of them is ours. We must get our slice (7.096773), never the taker's
    /// top-level total (2547.096773) across all four makers they crossed.
    #[test]
    fn multi_maker_row_books_only_our_slice() {
        let mut r = row(ROW_FOUR_MAKERS);
        r["maker_orders"][3]["order_id"] = json!(FILLED_ID);
        let top_level_size = json_f64(&r, "size").unwrap();
        assert!(
            (top_level_size - 2547.096773).abs() < 1e-9,
            "fixture drifted: top-level size is {top_level_size}"
        );

        let traded = parse_poly_order_trades(&feed(vec![r]), FILLED_ID);
        assert!((traded - 7.096773).abs() < 1e-9, "got {traded}");
        assert!(
            (traded - top_level_size).abs() > 1.0,
            "our slice must not equal the row's top-level size, or the test proves nothing"
        );
    }
}
