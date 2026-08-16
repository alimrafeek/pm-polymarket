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
const MARKET_FIELDS: [&str; 12] = [
    "clobTokenIds",
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

    for market in markets {
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
}
