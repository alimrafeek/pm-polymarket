use serde_json::{Map, Value};
use anyhow::{Context as _, Result, anyhow};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::types::{PolymarketFeeSchedule, PolymarketMarketDetails, DEFAULT_MIN_ORDER_SIZE};
use venue_core::log::{get_timestamp_ist, log_event};

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
/// rate, and dropping it silently zeroes the Polymarket fee leg in the arbitrage edge math.
const MARKET_FIELDS: [&str; 16] = [
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
];

/// Gamma event lookup for one config slug, wrapped in the venue-standard read retry (3 attempts,
/// 2 s apart) so a transient Gamma failure doesn't drop the whole Polymarket leg for the run.
pub async fn get_poly_market_data(slug: String) -> Result<Vec<PolymarketMarketDetails>> {
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
async fn fetch_poly_market_data(slug: &str) -> Result<Vec<PolymarketMarketDetails>> {
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
    let market_details = parse_market_detail(market_data).await?;

    Ok(market_details)
}

/// Rows per `GET /positions` page. The data API caps a response at 500, so this is one request
/// for any realistic account and the paging below is insurance rather than a hot path.
const POSITIONS_PAGE_LIMIT: usize = 500;

/// Current Polymarket positions for the `POLY_FUNDER` deposit wallet, as
/// `(token_id, shares, avg_price)` tuples — `asset`, `size`, and `avgPrice` from the
/// data API; everything else in the response is dropped. `avg_price` is the per-share
/// cost basis used to seed the Polymarket leg's entry price on restart.
///
/// Paged by `offset` until a short page arrives, because a truncated read is not a visibly
/// degraded one: `seed_positions` treats a token it never saw as flat, and a leg that looks flat
/// is handed a full `MAX_PAIRS_PER_TRADE` of fresh capacity on top of inventory it already holds
/// — and its real holdings are never offered for exit. Positions below `sizeThreshold` (1 share)
/// are still dropped by the venue, which is dust against a 40-pair cap.
pub async fn get_poly_positions() -> Result<Vec<(String, f64, f64)>> {
    let user = venue_core::trade::required_env("POLY_FUNDER")?;
    let client = reqwest::Client::new();

    let mut result: Vec<(String, f64, f64)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut offset: usize = 0;
    loop {
        let url = format!(
            "https://data-api.polymarket.com/positions?sizeThreshold=1&limit={POSITIONS_PAGE_LIMIT}&offset={offset}&sortBy=TOKENS&sortDirection=DESC&user={user}"
        );

        let response = client
            .get(&url)
            .header("accept", "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            println!("[{}] : Failed to fetch positions for user '{}'. Status: {}", get_timestamp_ist(), user, response.status());
            return Err(anyhow!("Request failed with status: {}", response.status()));
        }

        let json_response: Value = response.json().await?;

        let positions = json_response
            .as_array()
            .ok_or_else(|| anyhow!("Expected positions array in response"))?;
        let page_len = positions.len();

        let mut added = 0usize;
        for position in positions {
            let asset = position
                .get("asset")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("position missing asset"))?;
            let size = position
                .get("size")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("position missing size"))?;
            let avg_price = position
                .get("avgPrice")
                .and_then(|v| v.as_f64())
                .ok_or_else(|| anyhow!("position missing avgPrice"))?;
            // One token can only be held once, so a repeat means the venue served a page we have
            // already read. Keying on the id makes an ignored `offset` a loop that terminates
            // rather than one that spins forever re-reading page one.
            if !seen.insert(asset.to_string()) {
                continue;
            }
            result.push((asset.to_string(), size, avg_price));
            added += 1;
        }

        // A short page is the last page; a full page that taught us nothing new means `offset`
        // isn't being honoured, and reading on would not either.
        if page_len < POSITIONS_PAGE_LIMIT || added == 0 {
            return Ok(result);
        }
        offset += page_len;
    }
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
    let headers = trader.l2_headers(crate::trade::now_ts(), "GET", path, "")?;

    let response = trader
        .http
        .get(format!(
            "{}{}?asset_type=COLLATERAL&signature_type=3",
            crate::trade::HOST,
            path
        ))
        .headers(headers)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("GET {path} failed: HTTP {status}: {text}"));
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

/// Open orders resting on the Polymarket CLOB for the account the L2 credentials are bound to,
/// as raw JSON from GET /data/orders.
pub async fn get_poly_open_orders(trader: &crate::trade::PolyTrader) -> Result<Value> {
    let path = "/data/orders";
    let headers = trader.l2_headers(crate::trade::now_ts(), "GET", path, "")?;

    let response = trader
        .http
        .get(format!("{}{}", crate::trade::HOST, path))
        .headers(headers)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("GET {path} failed: HTTP {status}: {text}"));
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
    let headers = trader.l2_headers(crate::trade::now_ts(), "GET", path, "")?;

    let response = trader
        .http
        .get(format!("{}{}", crate::trade::HOST, path))
        .headers(headers)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("GET {path} failed: HTTP {status}: {text}"));
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
    let headers = trader.l2_headers(crate::trade::now_ts(), "DELETE", path, &body)?;

    let response = trader
        .http
        .delete(format!("{}{}", crate::trade::HOST, path))
        .headers(headers)
        .body(body)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("DELETE {path} failed: HTTP {status}: {text}"));
    }

    Ok(serde_json::from_str(&text)?)
}

async fn parse_market_detail(value: Value) -> Result<Vec<PolymarketMarketDetails>> {
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
    let mut untradeable = 0usize;

    for market in markets {
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
            untradeable += 1;
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
            market
                .get("groupItemTitle")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("groupItemTitle missing"))?
                .to_string()
        };

        let clob_token_ids_str = market
            .get("clobTokenIds")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("clobTokenIds missing"))?;

        let clob_token_ids: Vec<String> = serde_json::from_str(clob_token_ids_str)?;

        if clob_token_ids.len() < 2 {
            return Err(anyhow!("clobTokenIds must contain at least 2 elements"));
        }

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
            resolution_time,
            neg_risk,
            yes_token_id: clob_token_ids[0].clone(),
            no_token_id: clob_token_ids[1].clone(),
            yes_bids: Arc::new(Mutex::new(Vec::new())),
            yes_asks: Arc::new(Mutex::new(Vec::new())),
            no_bids: Arc::new(Mutex::new(Vec::new())),
            no_asks: Arc::new(Mutex::new(Vec::new())),
            tick_size: Arc::new(Mutex::new(tick_size)),
            min_order_size,
            fee_schedule,
        });
    }

    if untradeable > 0 {
        println!(
            "[{}] : [poly] {outer_slug}: skipped {untradeable} untradeable outcome(s) \
             (closed/archived/inactive/not accepting orders); {} kept",
            get_timestamp_ist(),
            result.len()
        );
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        let details = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1);
        let fs = details[0]
            .fee_schedule
            .expect("fees-enabled market must carry its schedule through the filter");
        assert_eq!(fs.rate, 0.05);
        assert_eq!(fs.exponent, 1.0);
        // rate × p(1−p) × shares, the on-chain-verified taker fee, on the live Spain ask.
        let fee = fs.taker_fee(0.5915, 1.0);
        assert!((fee - 0.05 * 0.5915 * 0.4085).abs() < 1e-12, "got {fee}");
    }

    /// Fee-free markets (feesEnabled false or absent) still parse with no schedule.
    #[tokio::test]
    async fn fee_free_market_parses_without_schedule() {
        let mut raw = raw_sports_market();
        raw["feesEnabled"] = json!(false);
        let picked = pick_fields(&raw, &MARKET_FIELDS);
        let event = json!({ "id": "1", "slug": "no-fees", "markets": [picked] });
        let details = parse_market_detail(event).await.unwrap();
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

        let details = parse_market_detail(event).await.unwrap();
        assert_eq!(details.len(), 1, "only the tradeable outcome survives");
        assert_eq!(details[0].market_slug, "Spain");
    }

    /// Markets that omit the tradeability flags are kept. `pick_fields` inserts every listed key,
    /// so an omitted flag arrives as JSON `null`, not as a missing key — the permissive defaults
    /// in the filter are what stop a null from emptying the universe.
    #[tokio::test]
    async fn absent_tradeability_flags_are_permissive() {
        let picked = pick_fields(&raw_sports_market(), &MARKET_FIELDS);
        assert!(picked["acceptingOrders"].is_null(), "fixture omits the flag");
        let event = json!({ "id": "1", "slug": "no-flags", "markets": [picked] });
        assert_eq!(parse_market_detail(event).await.unwrap().len(), 1);
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
