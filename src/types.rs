use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use venue_core::book::OrderBookLevel;

/// Taker fee schedule from the Gamma market-level `feeSchedule`. Fee rates vary per market
/// category (e.g. sports 3%, economics 5%), so this must be read per market, never hardcoded.
/// Taker fee in USDC: `rate * price * (1 - price) * shares` (per docs.polymarket.com/trading/fees,
/// verified against an on-chain fill; applied by the exchange at match time, not signed in orders).
#[derive(Debug, Clone, Copy)]
pub struct PolymarketFeeSchedule {
    pub rate: f64,
    pub exponent: f64,
    /// When true (the only observed value so far), makers pay nothing.
    #[allow(dead_code)]
    pub taker_only: bool,
    /// Share of collected taker fees paid back into the maker rebate pool, not a fee.
    #[allow(dead_code)]
    pub rebate_rate: f64,
}

impl PolymarketFeeSchedule {
    /// Taker fee in USDC for trading `shares` at per-share price `price`, per
    /// docs.polymarket.com/trading/fees: `rate * (p*(1-p))^exponent * shares`. The exponent is
    /// 1.0 in every schedule observed so far, which reduces to the on-chain-verified
    /// `rate * p * (1 - price) * shares`. Every leg we place is a marketable taker, so this
    /// always applies (makers, who pay nothing, are irrelevant here).
    pub fn taker_fee(&self, price: f64, shares: f64) -> f64 {
        self.rate * (price * (1.0 - price)).powf(self.exponent) * shares
    }
}

#[derive(Debug, Clone)]
pub struct PolymarketMarketDetails {
    // Mirrors OpinionMarketDetails.market_id; kept for order execution / debugging (not read yet).
    #[allow(dead_code)]
    pub market_id: u64,
    pub market_slug: String,
    /// Scheduled market resolution time as Unix epoch seconds, parsed from the market-level
    /// `endDate` (UTC). Uses the per-market value, not the event-level `endDate`, which can be years
    /// off. Directly comparable with `OpinionMarketDetails::resolution_time`. 0 if unavailable.
    pub resolution_time: i64,
    /// Whether this market settles through the neg-risk exchange contract, which changes the
    /// EIP-712 domain orders must be signed against. From the market-level `negRisk` flag.
    pub neg_risk: bool,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub yes_bids: Arc<Mutex<Vec<OrderBookLevel>>>,
    pub yes_asks: Arc<Mutex<Vec<OrderBookLevel>>>,
    pub no_bids: Arc<Mutex<Vec<OrderBookLevel>>>,
    pub no_asks: Arc<Mutex<Vec<OrderBookLevel>>>,
    pub tick_size: Arc<Mutex<f64>>,
    /// `None` when the market charges no fees (`feesEnabled` false or schedule absent).
    pub fee_schedule: Option<PolymarketFeeSchedule>,
}

/// A single Polymarket token's order book, keyed by `token_id` in the venue routing map.
/// The handles are clones of the `Arc`s living inside `PolymarketMarketDetails`, so writes
/// here are visible to whoever holds the corresponding `TradingState`.
#[derive(Clone)]
pub struct PolyTokenBook {
    pub bids: Arc<Mutex<Vec<OrderBookLevel>>>,
    pub asks: Arc<Mutex<Vec<OrderBookLevel>>>,
    pub tick_size: Arc<Mutex<f64>>,
    /// This market's change notifier; the WS handler fires it after applying an update.
    pub change: Arc<Notify>,
}
