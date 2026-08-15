//! Live order execution on Polymarket's CLOB V2 (Polygon, pUSD with 6 decimals) via the
//! **deposit wallet flow**.
//!
//! Venue rules encoded here: prices must sit on the market's tick grid, sizes are shares with at
//! most 2 decimals, and normal vs neg-risk markets settle through *different* exchange contracts
//! (changing the EIP-712 domain, hence the signature). V2 signs a slimmer order struct than V1 —
//! taker/expiration/nonce/feeRateBps are gone, replaced by a millisecond `timestamp` plus zeroed
//! `metadata`/`builder` bytes32 fields — against new exchange contracts with domain version "2".
//! `expiration` is still sent in the POST /order body but is no longer part of the signature.
//!
//! Since Polymarket's 2026 account migration the CLOB only accepts makers that are **deposit
//! wallets** (per-user ERC-1967 proxies holding pUSD; the address shown under the profile icon).
//! Legacy signature types 0/1/2 are refused with "maker address not allowed, please use the
//! deposit wallet flow". A deposit-wallet order sets `signatureType 3` (POLY_1271) with *both*
//! `maker` and `signer` set to the deposit wallet, and its signature is not a plain EIP-712 order
//! signature: the order hash is nested in an ERC-7739 `TypedDataSign` struct (inner domain
//! `DepositWallet/1`, verifyingContract = the deposit wallet) hashed under the exchange's domain
//! separator, signed by the owner EOA behind `POLY_PRIVATE_KEY`, and shipped with the ERC-7739
//! suffix (app separator ‖ contents hash ‖ contents type ‖ uint16 length) so the wallet contract
//! can validate it through ERC-1271.
//!
//! L2 HMAC auth headers must carry API credentials bound to the *deposit wallet*, not the EOA —
//! the CLOB enforces `order.signer == api_key.address`. `derive_api_key`/`create_api_key` mint
//! such credentials by signing the ClobAuth L1 message for the deposit wallet with the owner EOA.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{address, keccak256, Address, B256, U256};
use alloy::sol;
use alloy::sol_types::SolStruct as _;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use hmac::{Hmac, Mac as _};
use k256::ecdsa::SigningKey;
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use serde::ser::{SerializeStruct as _, Serializer};
use serde::Serialize;
use serde_with::{serde_as, DefaultOnError, DisplayFromStr};
use sha2::Sha256;
use std::str::FromStr as _;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use venue_core::log::{get_timestamp_ist, log_event};
use venue_core::trade::{
    generate_salt, parse_private_key, required_env, retry_on_connect, sign_digest_hex,
    venue_http_builder, Side, VenueRejected,
};

pub(crate) const HOST: &str = "https://clob.polymarket.com";
const CHAIN_ID: u64 = 137; // Polygon
const ORDER_NAME: &str = "Polymarket CTF Exchange";
const ORDER_VERSION: &str = "2";

// Normal and neg-risk markets verify against different exchange contracts; signing against the
// wrong one gets the order rejected. The per-market negRisk flag picks the contract.
const EXCHANGE_CONTRACT_NORMAL_V2: Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const EXCHANGE_CONTRACT_NEG_RISK_V2: Address =
    address!("0xe2222d279d744050d28e00520010520000310F59");

/// pUSD, like USDC before it, is a 6-decimals token.
const COLLATERAL_DECIMALS: u32 = 6;
/// Sizes (shares) allow at most 2 decimal places.
const LOT_SIZE_SCALE: u32 = 2;
/// ERC-1271 contract signature — maker *and* signer are the deposit wallet; the owner EOA
/// produces the ERC-7739-wrapped signature the wallet contract validates.
const SIGNATURE_TYPE_POLY_1271: u8 = 3;

/// The deposit wallet proxy's own EIP-712 domain (its salt is zero).
const DEPOSIT_WALLET_DOMAIN_NAME: &str = "DepositWallet";
const DEPOSIT_WALLET_DOMAIN_VERSION: &str = "1";

// L1 auth (create/derive API keys): a ClobAuth EIP-712 attestation. The `address` field is the
// account the credentials get bound to — the deposit wallet — while the ECDSA signature comes
// from its owner EOA.
const CLOB_AUTH_DOMAIN_TYPE: &str = "EIP712Domain(string name,string version,uint256 chainId)";
const CLOB_AUTH_DOMAIN_NAME: &str = "ClobAuthDomain";
const CLOB_AUTH_DOMAIN_VERSION: &str = "1";
const CLOB_AUTH_TYPE: &str =
    "ClobAuth(address address,string timestamp,uint256 nonce,string message)";
const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

// Auth header names (L1 and L2 share address/signature/timestamp).
const POLY_ADDRESS: &str = "POLY_ADDRESS";
const POLY_API_KEY: &str = "POLY_API_KEY";
const POLY_PASSPHRASE: &str = "POLY_PASSPHRASE";
const POLY_SIGNATURE: &str = "POLY_SIGNATURE";
const POLY_TIMESTAMP: &str = "POLY_TIMESTAMP";
const POLY_NONCE: &str = "POLY_NONCE";

sol! {
    /// CLOB V2 EIP-712 order — field order matters for hashing, and so does the *name*: the
    /// canonical type string is `Order(...)`, and under ERC-7739 that string is embedded verbatim
    /// in both the TypedDataSign type hash and the signature suffix, so the sol! ident must stay
    /// `Order`. Unlike the V1 struct shared with OpinionLabs, V2 drops
    /// taker/expiration/nonce/feeRateBps and adds timestamp (millis) plus optional
    /// metadata/builder bytes32 fields (zero when unused).
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8   side;
        uint8   signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }
}

/// The matching engine treats a GTD order as expired one minute *before* the expiration on the
/// wire, so the timestamp sent carries this buffer on top of the lifetime the caller asked for.
const GTD_SECURITY_BUFFER_SECS: u64 = 60;
/// The CLOB refuses a GTD expiration less than three minutes out; with the buffer above, that
/// makes two minutes the shortest book lifetime a GTD order can actually be given.
const GTD_MIN_LIFETIME_SECS: u64 = 120;

/// Which of the CLOB's four `orderType` behaviours an order is sent with, carrying the extra
/// inputs each one needs. Chosen per order by the caller — the venue module has no default.
///
/// The two immediate types never rest: whatever doesn't cross straight away is killed by the
/// venue, and an order that crosses *nothing at all* is rejected outright (HTTP 400 "no orders
/// found to match…") rather than acked empty. The two resting types do the opposite — they ack
/// success with `status: "live"` and zero fill amounts when nothing crosses, so their caller must
/// read the ack's amounts rather than assume the size it sent traded, and must cancel what rests
/// (`cancel_poly_order`) if it no longer wants it.
///
/// `post_only` is a field of the resting variants only, because it is meaningless on the others:
/// it tells the venue to reject the order outright rather than let any part of it take, which is
/// the exact opposite of what FAK/FOK ask for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolyOrderType {
    /// Fill-and-kill: takes whatever is available at the limit price or better and the venue
    /// cancels the remainder. A partial fill still acks success.
    Fak,
    /// Fill-or-kill: the whole size fills immediately or none of it does — no partial fills.
    Fok,
    /// Good-til-cancelled: fills whatever crosses and rests the remainder on the book until it is
    /// cancelled or the market resolves.
    Gtc { post_only: bool },
    /// Good-til-date: a [`PolyOrderType::Gtc`] the venue cancels once it has been on the book for
    /// `lifetime_secs`. The wire `expiration` is `now + 60 + lifetime_secs` — see
    /// [`GTD_SECURITY_BUFFER_SECS`] — and lifetimes under [`GTD_MIN_LIFETIME_SECS`] are refused
    /// here rather than sent to be refused by the venue.
    Gtd { lifetime_secs: u64, post_only: bool },
}

impl PolyOrderType {
    /// The string the CLOB's `orderType` field takes.
    pub fn wire(self) -> &'static str {
        match self {
            Self::Fak => "FAK",
            Self::Fok => "FOK",
            Self::Gtc { .. } => "GTC",
            Self::Gtd { .. } => "GTD",
        }
    }

    /// Maker-only: the venue rejects the order instead of letting any part of it take.
    pub fn post_only(self) -> bool {
        match self {
            Self::Fak | Self::Fok => false,
            Self::Gtc { post_only } | Self::Gtd { post_only, .. } => post_only,
        }
    }

    /// Whether an unfilled remainder stays on the book — i.e. whether the caller is responsible
    /// for cancelling it.
    pub fn rests(self) -> bool {
        matches!(self, Self::Gtc { .. } | Self::Gtd { .. })
    }

    /// The `expiration` the order body carries: zero for everything but GTD, whose wire value is
    /// the requested lifetime plus the venue's one-minute security threshold.
    fn expiration(self, now_secs: i64) -> Result<U256> {
        let Self::Gtd { lifetime_secs, .. } = self else {
            return Ok(U256::ZERO);
        };
        if lifetime_secs < GTD_MIN_LIFETIME_SECS {
            return Err(anyhow!(
                "GTD lifetime {lifetime_secs}s is below the CLOB's minimum of \
                 {GTD_MIN_LIFETIME_SECS}s (its expiration must be at least \
                 {}s out, of which {GTD_SECURITY_BUFFER_SECS}s is the security threshold)",
                GTD_MIN_LIFETIME_SECS + GTD_SECURITY_BUFFER_SECS
            ));
        }
        let now = u64::try_from(now_secs).map_err(|_| anyhow!("system clock is before the epoch"))?;
        Ok(U256::from(
            now.saturating_add(GTD_SECURITY_BUFFER_SECS)
                .saturating_add(lifetime_secs),
        ))
    }
}

/// Subset of the CLOB's POST /order response used for logging and success checks.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct PolyOrderAck {
    #[serde(default)]
    pub success: bool,
    #[serde(default, rename = "orderID")]
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    /// How much of the order's maker side actually traded: pUSD spent on a BUY, shares sold on a
    /// SELL. The venue sends a plain decimal string in human units (e.g. "1.61" — the docs claim
    /// 6-decimal fixed-math integers, but live responses return plain decimals; see
    /// sample_response_poly.jsonc), parsed to f64 here for position math. `None` when absent or
    /// unparseable rather than an ack error — a mis-shaped amount on an otherwise accepted order
    /// must not surface as a rejection, or the fill it reports would go untracked. With FAK/FOK
    /// orders the killed remainder never fills later, so comparing the share-side amount against
    /// what was sent is the partial-fill check; with the resting types (GTC/GTD) these amounts are
    /// only what crossed *on entry* and the rest may still fill afterwards.
    #[serde(default, rename = "makingAmount")]
    #[serde_as(as = "DefaultOnError<Option<DisplayFromStr>>")]
    pub making_amount: Option<f64>,
    /// The taker-side mirror of `making_amount`: shares received on a BUY, pUSD received on a
    /// SELL. Same encoding and `None` semantics.
    #[serde(default, rename = "takingAmount")]
    #[serde_as(as = "DefaultOnError<Option<DisplayFromStr>>")]
    pub taking_amount: Option<f64>,
}

impl PolyOrderAck {
    /// Shares that actually traded: the share side of the making/taking amounts — `taking_amount`
    /// on a BUY (shares received), `making_amount` on a SELL (shares given). `None` when the ack
    /// didn't carry a usable amount.
    pub fn filled_shares(&self, side: Side) -> Option<f64> {
        match side {
            Side::Buy => self.taking_amount,
            Side::Sell => self.making_amount,
        }
    }
}

pub struct PolyTrader {
    pub(crate) http: reqwest::Client,
    signing_key: SigningKey,
    eoa: Address,
    api_key: Uuid,
    api_secret_b64url: String,
    passphrase: String,
    funder: Address,
}

impl PolyTrader {
    /// Env vars: `POLY_PRIVATE_KEY` (the owner EOA key that signs orders), `POLY_API_KEY` /
    /// `POLY_API_SECRET` / `POLY_API_PASSPHRASE` (the L2 credential triple — must be bound to
    /// the deposit wallet; mint with the `poly-derive-key` manual command), and `POLY_FUNDER`
    /// (the deposit wallet holding pUSD and positions — the address under the profile icon on
    /// polymarket.com, *not* the deposit-window bridge address and not the EOA).
    pub fn from_env() -> Result<Self> {
        let (signing_key, eoa) = parse_private_key(&required_env("POLY_PRIVATE_KEY")?)
            .context("POLY_PRIVATE_KEY invalid")?;
        let api_key = Uuid::parse_str(&required_env("POLY_API_KEY")?)
            .context("POLY_API_KEY must be a UUID")?;
        let api_secret_b64url = required_env("POLY_API_SECRET")?;
        // Fail on a malformed secret at startup, not on the first live order.
        URL_SAFE
            .decode(&api_secret_b64url)
            .context("POLY_API_SECRET is not base64url")?;
        let passphrase = required_env("POLY_API_PASSPHRASE")?;
        let funder = Address::from_str(&required_env("POLY_FUNDER")?)
            .context("POLY_FUNDER must be an EVM address")?;

        let mut default_headers = HeaderMap::new();
        default_headers.insert("User-Agent", HeaderValue::from_static("pm-arbitrage"));
        default_headers.insert("Accept", HeaderValue::from_static("*/*"));
        default_headers.insert("Connection", HeaderValue::from_static("keep-alive"));
        default_headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        let http = venue_http_builder()
            .default_headers(default_headers)
            .build()?;

        Ok(Self {
            http,
            signing_key,
            eoa,
            api_key,
            api_secret_b64url,
            passphrase,
            funder,
        })
    }

    pub fn eoa(&self) -> Address {
        self.eoa
    }

    pub fn funder(&self) -> Address {
        self.funder
    }

    /// Place an order for `size` shares at limit `price`, with `order_type` deciding what the
    /// venue does with whatever doesn't cross immediately — see [`PolyOrderType`], which the
    /// caller picks per order. `price` is snapped to the market's tick grid and `size` truncated
    /// to the 2-decimal lot size before signing. Ok only when the CLOB acks with success —
    /// anything else (HTTP error, success=false, off-grid price, an immediate order that crossed
    /// nothing, a post-only order that would have taken) is an Err carrying the venue's response
    /// text.
    ///
    /// Ok is *not* proof of a fill: with [`PolyOrderType::Fak`] a partial fill acks success, and
    /// with the resting types an order that crossed nothing acks success too (`status: "live"`).
    /// Read the ack's [`PolyOrderAck::filled_shares`] for what actually traded, and remember that
    /// anything a resting order leaves on the book is this caller's to cancel.
    ///
    /// The send is replayed **only** when it died during TCP/TLS connect, via
    /// [`retry_on_connect`] — see [`Self::post_order_once`] for why that is safe here.
    pub async fn place_order(
        &self,
        market_subtitle: &str,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
        neg_risk: bool,
        order_type: PolyOrderType,
    ) -> Result<PolyOrderAck> {
        log_event(
            market_subtitle,
            &format!(
                "place_order : {side:?} {size} @ {price} (tick {tick_size}, {} post_only={})",
                order_type.wire(),
                order_type.post_only()
            ),
        );
        let token_id = U256::from_str(token_id)
            .map_err(|e| anyhow!("invalid Polymarket token id: {e:?}"))?;
        let tick_size = tick_size.normalize();
        let price = price.round_dp(tick_size.scale()).normalize();
        let size = size.trunc_with_scale(LOT_SIZE_SCALE).normalize();
        // Before signing: a lifetime the CLOB would refuse is worth catching here, not after the
        // signature work.
        let expiration = order_type.expiration(now_ts())?;

        let order = build_limit_order(token_id, price, size, side, tick_size, self.funder)?;

        let exchange_contract = if neg_risk {
            EXCHANGE_CONTRACT_NEG_RISK_V2
        } else {
            EXCHANGE_CONTRACT_NORMAL_V2
        };
        let domain = Eip712Domain {
            name: Some(ORDER_NAME.into()),
            version: Some(ORDER_VERSION.into()),
            chain_id: Some(U256::from(CHAIN_ID)),
            verifying_contract: Some(exchange_contract),
            ..Eip712Domain::default()
        };
        let signature_hex = sign_order_erc7739(&self.signing_key, &order, &domain)?;

        let payload = SignedOrderPayload {
            order,
            // Sent on the wire (only GTD uses it) but not part of the V2 signature, which is why
            // it can be stamped onto an order that was already signed without it.
            expiration,
            signature_hex,
            order_type,
            owner: self.api_key,
            post_only: order_type.post_only(),
            defer_exec: false,
        };
        let body = serde_json::to_string(&payload)?;

        let what = format!("polymarket order {} {side:?} {size} @ {price}", order_type.wire());
        let resp = retry_on_connect(market_subtitle, &what, || self.post_order_once(&body)).await?;

        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        // Raw venue response, printed unconditionally: the parsed ack keeps only a subset of the
        // fields, and rejection bodies otherwise surface only through the returned Err.
        println!("[{}] : [poly] POST /order response: HTTP {status}: {text}", get_timestamp_ist());
        log_event(market_subtitle, &format!("POST /order response: HTTP {status}: {text}"));
        if !status.is_success() {
            // Typed cause alongside the message: a 4xx here is proof the CLOB matched nothing,
            // which is what lets a flatten retry safely (see `is_definitive_no_fill`).
            return Err(anyhow::Error::new(VenueRejected { status: status.as_u16() })
                .context(format!("Polymarket order rejected: HTTP {status}: {text}")));
        }
        let ack: PolyOrderAck = serde_json::from_str(&text)
            .with_context(|| format!("unparseable Polymarket ack: {text}"))?;
        if !ack.success {
            log_event(market_subtitle, &format!("Polymarket order not accepted. PolyOrderAck {:#?}", ack));
            return Err(anyhow!("Polymarket order not accepted: {text}"));
        }
        Ok(ack)
    }

    /// One `POST /order` send, as [`Self::place_order`]'s retryable unit.
    ///
    /// `body` is the already-signed order and is reused byte-identically across attempts — same
    /// salt, same signature — so a replay is recognisable to the CLOB as the same order rather
    /// than a second one. The L2 headers, by contrast, must be rebuilt per attempt: the HMAC
    /// covers a timestamp the venue rejects once stale.
    async fn post_order_once(&self, body: &str) -> Result<reqwest::Response> {
        let headers = self.l2_headers(now_ts(), "POST", "/order", body)?;
        self.http
            .post(format!("{HOST}/order"))
            .headers(headers)
            .body(body.to_string())
            .send()
            .await
            .context("POST /order to Polymarket failed")
    }

    /// One cheap round trip whose only job is to keep this client's pooled connection alive, so an
    /// order never has to open a cold one (see [`crate::trade_common::venue_http_builder`] for why
    /// that matters). `/balance-allowance` is the same host, the same pooled connection and the
    /// same L2 signing path `/order` uses, so a heartbeat that passes is evidence the next order
    /// can actually be sent. The balance it returns is discarded — the balance tracker owns that.
    pub async fn ping(&self) -> Result<()> {
        let path = "/balance-allowance";
        let headers = self.l2_headers(now_ts(), "GET", path, "")?;
        let response = self
            .http
            .get(format!("{HOST}{path}?asset_type=COLLATERAL&signature_type=3"))
            .headers(headers)
            .send()
            .await
            .context("GET /balance-allowance to Polymarket failed")?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Polymarket heartbeat rejected: HTTP {status}: {text}"));
        }
        Ok(())
    }

    /// Polymarket L2 auth: HMAC-SHA256 over `{timestamp}{method}{path}{body}` with the base64url
    /// API secret, sent alongside the address/key/passphrase/timestamp headers. `POLY_ADDRESS`
    /// is the account the credentials are bound to. The CLOB's L1 endpoints reject contract
    /// addresses outright (strict ecrecover — verified empirically), so credentials can only be
    /// EOA-bound and this header carries the EOA even in the deposit-wallet flow.
    pub(crate) fn l2_headers(
        &self,
        timestamp: i64,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<HeaderMap> {
        let decoded = URL_SAFE
            .decode(&self.api_secret_b64url)
            .context("failed to base64url-decode POLY_API_SECRET")?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&decoded)
            .map_err(|e| anyhow!("failed to init HMAC: {e}"))?;
        mac.update(format!("{timestamp}{method}{path}{body}").as_bytes());
        let sig = URL_SAFE.encode(mac.finalize().into_bytes());

        let mut h = HeaderMap::new();
        h.insert(POLY_ADDRESS, self.eoa.to_string().parse()?);
        h.insert(POLY_API_KEY, self.api_key.to_string().parse()?);
        h.insert(POLY_PASSPHRASE, self.passphrase.parse()?);
        h.insert(POLY_SIGNATURE, sig.parse()?);
        h.insert(POLY_TIMESTAMP, timestamp.to_string().parse()?);
        Ok(h)
    }

    /// Polymarket L1 auth: a ClobAuth EIP-712 attestation whose `address` field is the account
    /// the credentials get bound to. For the deposit wallet that account is a contract, so the
    /// attestation is signed with the same ERC-7739 wrapping the wallet's ERC-1271 check needs
    /// (`wrapped: true`); a plain EOA signature is kept for probing/legacy (`wrapped: false`).
    fn l1_headers(&self, timestamp: i64, account: Address, wrapped: bool) -> Result<HeaderMap> {
        let nonce = 0u64;
        let ts = timestamp.to_string();
        let sig = if wrapped {
            erc7739_sign(
                &self.signing_key,
                account,
                clob_auth_domain_separator(),
                clob_auth_struct_hash(account, &ts, nonce),
                CLOB_AUTH_TYPE,
            )?
        } else {
            sign_digest_hex(&self.signing_key, &clob_auth_digest(account, &ts, nonce))?
        };

        let mut h = HeaderMap::new();
        h.insert(POLY_ADDRESS, account.to_string().parse()?);
        h.insert(POLY_SIGNATURE, sig.parse()?);
        h.insert(POLY_TIMESTAMP, ts.parse()?);
        h.insert(POLY_NONCE, nonce.to_string().parse()?);
        Ok(h)
    }

    #[allow(dead_code)]
    /// Mint a fresh L2 credential triple bound to the deposit wallet (POST /auth/api-key).
    pub async fn create_api_key(&self) -> Result<PolyApiCreds> {
        self.api_key_request(reqwest::Method::POST, "/auth/api-key", self.funder, true)
            .await
    }

    #[allow(dead_code)]
    /// Recover the existing deposit-wallet credential triple (GET /auth/derive-api-key).
    pub async fn derive_api_key(&self) -> Result<PolyApiCreds> {
        self.api_key_request(reqwest::Method::GET, "/auth/derive-api-key", self.funder, true)
            .await
    }

    /// Probe variant used by the manual `poly-derive-key` command to pin down which L1 binding
    /// the CLOB accepts: any `account` (deposit wallet or EOA), wrapped or plain signature.
    pub async fn derive_api_key_variant(
        &self,
        account: Address,
        wrapped: bool,
    ) -> Result<PolyApiCreds> {
        self.api_key_request(reqwest::Method::GET, "/auth/derive-api-key", account, wrapped)
            .await
    }

    #[allow(dead_code)]
    /// Create-then-derive: create fails once a key already exists for the wallet, in which case
    /// deriving returns the standing one.
    pub async fn create_or_derive_api_key(&self) -> Result<PolyApiCreds> {
        match self.create_api_key().await {
            Ok(creds) => Ok(creds),
            Err(create_err) => self
                .derive_api_key()
                .await
                .with_context(|| format!("create failed ({create_err:#}); derive also failed")),
        }
    }

    async fn api_key_request(
        &self,
        method: reqwest::Method,
        path: &str,
        account: Address,
        wrapped: bool,
    ) -> Result<PolyApiCreds> {
        let headers = self.l1_headers(now_ts(), account, wrapped)?;
        let resp = self
            .http
            .request(method.clone(), format!("{HOST}{path}"))
            .headers(headers)
            .send()
            .await
            .with_context(|| format!("{method} {path} to Polymarket failed"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("{method} {path} rejected: HTTP {status}: {text}"));
        }
        serde_json::from_str(&text)
            .with_context(|| format!("unparseable API-key response: {text}"))
    }
}

/// L2 credential triple as returned by the auth endpoints; bound to whatever address was in the
/// L1 headers that minted it.
#[derive(Debug, serde::Deserialize)]
pub struct PolyApiCreds {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

pub(crate) fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs() as i64
}

/// V2 orders carry their creation time in *milliseconds* inside the signed struct.
fn now_ts_millis() -> U256 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis();
    U256::from(ms)
}

// SDK-like: normalize, trunc to 6dp, take mantissa => integer fixed-6
fn to_fixed_u128(d: Decimal) -> u128 {
    d.normalize()
        .trunc_with_scale(COLLATERAL_DECIMALS)
        .mantissa()
        .unsigned_abs()
        .try_into()
        .expect("value doesn't fit u128")
}

fn build_limit_order(
    token_id: U256,
    price: Decimal,
    size: Decimal,
    side: Side,
    tick_size: Decimal,
    deposit_wallet: Address,
) -> Result<Order> {
    if size <= Decimal::ZERO {
        return Err(anyhow!("size must be > 0"));
    }
    let tick_decimals = tick_size.scale();
    if price.scale() > tick_decimals {
        return Err(anyhow!("price {price} is finer than tick size {tick_size}"));
    }
    if price < tick_size || price > Decimal::ONE - tick_size {
        return Err(anyhow!("price {price} out of bounds for tick size {tick_size}"));
    }

    // SDK truncation: notional truncated to (tick_decimals + LOT_SIZE_SCALE).
    let (taker_amount, maker_amount) = match side {
        Side::Buy => (
            size,
            (size * price).trunc_with_scale(tick_decimals + LOT_SIZE_SCALE),
        ),
        Side::Sell => (
            (size * price).trunc_with_scale(tick_decimals + LOT_SIZE_SCALE),
            size,
        ),
    };

    Ok(Order {
        salt: U256::from(generate_salt()),
        // Deposit wallet flow: the wallet contract is both maker and signer; the owner EOA
        // appears only in the ECDSA signature the wallet validates via ERC-1271.
        maker: deposit_wallet,
        signer: deposit_wallet,
        tokenId: token_id,
        makerAmount: U256::from(to_fixed_u128(maker_amount)),
        takerAmount: U256::from(to_fixed_u128(taker_amount)),
        side: side.as_u8(),
        signatureType: SIGNATURE_TYPE_POLY_1271,
        timestamp: now_ts_millis(),
        // Builder attribution and custom metadata are unused; V2 accepts zero bytes32 for both.
        metadata: B256::ZERO,
        builder: B256::ZERO,
    })
}

/// ERC-7739 "wrapped typed data" signature for a deposit-wallet order.
///
/// Instead of signing the order digest directly, the EOA signs
/// `keccak256(0x1901 ‖ appDomainSeparator ‖ hashStruct(TypedDataSign))`, where the TypedDataSign
/// struct nests the order hash as `contents` alongside the *deposit wallet's* domain fields
/// (name "DepositWallet", version "1", chainId, verifyingContract = wallet, zero salt). The wire
/// signature then appends `appDomainSeparator ‖ contentsHash ‖ contentsType ‖ uint16(len)` so the
/// wallet contract can rebuild and verify the same digest through ERC-1271.
fn sign_order_erc7739(
    key: &SigningKey,
    order: &Order,
    exchange_domain: &Eip712Domain,
) -> Result<String> {
    erc7739_sign(
        key,
        order.signer,
        exchange_domain.separator(),
        order.eip712_hash_struct(),
        &Order::eip712_root_type(),
    )
}

/// Generic ERC-7739 wrap-and-sign for any EIP-712 struct destined for the deposit wallet's
/// ERC-1271 validation. `contents_type` is the struct's full type string, e.g. `Order(...)`;
/// its name (text before the parenthesis) is spliced into the TypedDataSign type.
fn erc7739_sign(
    key: &SigningKey,
    deposit_wallet: Address,
    app_separator: B256,
    contents_hash: B256,
    contents_type: &str,
) -> Result<String> {
    let contents_name = contents_type
        .split('(')
        .next()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| anyhow!("malformed contents type string: {contents_type}"))?;
    // "TypedDataSign(<Name> contents,...)<Name>(...)" — referenced struct types are appended.
    let typed_data_sign_type = format!(
        "TypedDataSign({contents_name} contents,string name,string version,uint256 chainId,\
         address verifyingContract,bytes32 salt){contents_type}"
    );

    // abi.encode(bytes32,bytes32,bytes32,bytes32,uint256,address,bytes32): 7 static words.
    let mut words = Vec::with_capacity(7 * 32);
    words.extend_from_slice(keccak256(typed_data_sign_type.as_bytes()).as_slice());
    words.extend_from_slice(contents_hash.as_slice());
    words.extend_from_slice(keccak256(DEPOSIT_WALLET_DOMAIN_NAME.as_bytes()).as_slice());
    words.extend_from_slice(keccak256(DEPOSIT_WALLET_DOMAIN_VERSION.as_bytes()).as_slice());
    words.extend_from_slice(B256::from(U256::from(CHAIN_ID)).as_slice());
    words.extend_from_slice(B256::left_padding_from(deposit_wallet.as_slice()).as_slice());
    words.extend_from_slice(B256::ZERO.as_slice());
    let typed_data_sign_hash = keccak256(&words);

    let mut preimage = Vec::with_capacity(2 + 64);
    preimage.extend_from_slice(b"\x19\x01");
    preimage.extend_from_slice(app_separator.as_slice());
    preimage.extend_from_slice(typed_data_sign_hash.as_slice());
    let digest = keccak256(&preimage);

    let inner_sig = sign_digest_hex(key, &digest)?;
    let contents_type_len = u16::try_from(contents_type.len())
        .map_err(|_| anyhow!("contents type string too long"))?;
    Ok(format!(
        "{inner_sig}{}{}{}{contents_type_len:04x}",
        hex::encode(app_separator),
        hex::encode(contents_hash),
        hex::encode(contents_type.as_bytes()),
    ))
}

/// ClobAuth domain separator — the domain has no verifyingContract, so its type string (and
/// hence separator) is name/version/chainId only.
fn clob_auth_domain_separator() -> B256 {
    let mut dom = Vec::with_capacity(4 * 32);
    dom.extend_from_slice(keccak256(CLOB_AUTH_DOMAIN_TYPE.as_bytes()).as_slice());
    dom.extend_from_slice(keccak256(CLOB_AUTH_DOMAIN_NAME.as_bytes()).as_slice());
    dom.extend_from_slice(keccak256(CLOB_AUTH_DOMAIN_VERSION.as_bytes()).as_slice());
    dom.extend_from_slice(B256::from(U256::from(CHAIN_ID)).as_slice());
    keccak256(&dom)
}

/// hashStruct of the ClobAuth L1 attestation binding API credentials to `account`.
fn clob_auth_struct_hash(account: Address, timestamp: &str, nonce: u64) -> B256 {
    let mut st = Vec::with_capacity(5 * 32);
    st.extend_from_slice(keccak256(CLOB_AUTH_TYPE.as_bytes()).as_slice());
    st.extend_from_slice(B256::left_padding_from(account.as_slice()).as_slice());
    st.extend_from_slice(keccak256(timestamp.as_bytes()).as_slice());
    st.extend_from_slice(B256::from(U256::from(nonce)).as_slice());
    st.extend_from_slice(keccak256(CLOB_AUTH_MESSAGE.as_bytes()).as_slice());
    keccak256(&st)
}

/// Plain EIP-712 digest of the ClobAuth attestation (EOA signature path).
fn clob_auth_digest(account: Address, timestamp: &str, nonce: u64) -> B256 {
    let mut preimage = Vec::with_capacity(2 + 64);
    preimage.extend_from_slice(b"\x19\x01");
    preimage.extend_from_slice(clob_auth_domain_separator().as_slice());
    preimage.extend_from_slice(clob_auth_struct_hash(account, timestamp, nonce).as_slice());
    keccak256(&preimage)
}

// The CLOB parses the salt as a JSON *number* (all other uint256s go as strings).
fn ser_salt<S: Serializer>(value: &U256, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    let v: u64 = (*value)
        .try_into()
        .map_err(|e| serde::ser::Error::custom(format!("salt does not fit u64: {e}")))?;
    serializer.serialize_u64(v)
}

fn ser_b256_hex<S: Serializer>(value: &B256, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("0x{}", hex::encode(value.as_slice())))
}

/// Final request body: `{ order: {..., expiration, signature}, owner, orderType, postOnly,
/// deferExec }`.
struct SignedOrderPayload {
    order: Order,
    expiration: U256,
    signature_hex: String,
    order_type: PolyOrderType,
    owner: Uuid,
    post_only: bool,
    defer_exec: bool,
}

#[serde_as]
#[derive(Serialize)]
struct OrderWithSignature<'a> {
    #[serde(serialize_with = "ser_salt")]
    salt: &'a U256,
    maker: &'a Address,
    signer: &'a Address,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "tokenId")]
    token_id: &'a U256,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "makerAmount")]
    maker_amount: &'a U256,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "takerAmount")]
    taker_amount: &'a U256,
    side: Side,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    #[serde_as(as = "DisplayFromStr")]
    timestamp: &'a U256,
    #[serde(serialize_with = "ser_b256_hex")]
    metadata: &'a B256,
    #[serde(serialize_with = "ser_b256_hex")]
    builder: &'a B256,
    #[serde_as(as = "DisplayFromStr")]
    expiration: &'a U256,
    signature: &'a str,
}

impl Serialize for SignedOrderPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut st = serializer.serialize_struct("SignedOrder", 5)?;

        let side = match self.order.side {
            0 => Side::Buy,
            1 => Side::Sell,
            other => {
                return Err(serde::ser::Error::custom(format!(
                    "invalid side in order: {other}"
                )))
            }
        };

        let order_with_sig = OrderWithSignature {
            salt: &self.order.salt,
            maker: &self.order.maker,
            signer: &self.order.signer,
            token_id: &self.order.tokenId,
            maker_amount: &self.order.makerAmount,
            taker_amount: &self.order.takerAmount,
            side,
            signature_type: self.order.signatureType,
            timestamp: &self.order.timestamp,
            metadata: &self.order.metadata,
            builder: &self.order.builder,
            expiration: &self.expiration,
            signature: &self.signature_hex,
        };

        st.serialize_field("order", &order_with_sig)?;
        st.serialize_field("owner", &self.owner)?;
        st.serialize_field("orderType", self.order_type.wire())?;
        st.serialize_field("postOnly", &self.post_only)?;
        st.serialize_field("deferExec", &self.defer_exec)?;
        st.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Anvil/Hardhat dev key #0 — public knowledge, safe to embed. Used to exercise the ERC-7739
    // signing path end to end.
    const ANVIL_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const DEPOSIT: Address = address!("0xc13a440ca2Cf647d91093b7743A546620Da51F10");

    /// SELL 5 shares at 0.76 with tick 0.01: maker gives 5.000000 shares (5_000_000 fixed-6),
    /// taker pays 3.80 pUSD (3_800_000). Deposit-wallet flow: maker == signer == wallet, type 3.
    #[test]
    fn sell_amounts_and_deposit_wallet_fields() {
        let order = build_limit_order(
            U256::from(1u8),
            dec!(0.76),
            dec!(5),
            Side::Sell,
            dec!(0.01),
            DEPOSIT,
        )
        .unwrap();
        assert_eq!(order.makerAmount, U256::from(5_000_000u64));
        assert_eq!(order.takerAmount, U256::from(3_800_000u64));
        assert_eq!(order.side, 1);
        assert_eq!(order.signatureType, SIGNATURE_TYPE_POLY_1271);
        assert_eq!(order.maker, DEPOSIT);
        assert_eq!(order.signer, DEPOSIT);
        // V2 extras: creation timestamp in millis, zeroed metadata/builder.
        assert!(order.timestamp > U256::ZERO);
        assert_eq!(order.metadata, B256::ZERO);
        assert_eq!(order.builder, B256::ZERO);
    }

    #[test]
    fn buy_amounts_mirror_sell() {
        let order = build_limit_order(
            U256::from(1u8),
            dec!(0.76),
            dec!(5),
            Side::Buy,
            dec!(0.01),
            DEPOSIT,
        )
        .unwrap();
        assert_eq!(order.makerAmount, U256::from(3_800_000u64)); // pUSD paid
        assert_eq!(order.takerAmount, U256::from(5_000_000u64)); // shares received
        assert_eq!(order.side, 0);
    }

    #[test]
    fn price_must_sit_on_tick_grid() {
        // 0.765 is finer than a 0.01 tick.
        assert!(build_limit_order(
            U256::from(1u8),
            dec!(0.765),
            dec!(5),
            Side::Buy,
            dec!(0.01),
            DEPOSIT
        )
        .is_err());
        // Out of the [tick, 1 - tick] band.
        assert!(build_limit_order(
            U256::from(1u8),
            dec!(1.0),
            dec!(5),
            Side::Buy,
            dec!(0.01),
            DEPOSIT
        )
        .is_err());
    }

    /// Fractional sizes: 12.34 shares at 0.31 on a 0.001 tick — notional 3.8254 truncates at
    /// tick_decimals + 2 = 5 places (no truncation needed here) → 3_825_400 fixed-6.
    #[test]
    fn fractional_size_truncation() {
        let order = build_limit_order(
            U256::from(1u8),
            dec!(0.31),
            dec!(12.34),
            Side::Buy,
            dec!(0.001),
            DEPOSIT,
        )
        .unwrap();
        assert_eq!(order.takerAmount, U256::from(12_340_000u64));
        assert_eq!(order.makerAmount, U256::from(3_825_400u64));
    }

    /// The ERC-7739 wrapper embeds this exact string in the type hash and signature suffix, and
    /// the CLOB/wallet recompute it independently — a struct rename or field drift breaks live
    /// signature validation, not just tests.
    #[test]
    fn order_type_string_is_canonical() {
        assert_eq!(
            Order::eip712_root_type(),
            "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)"
        );
    }

    /// Wire signature must decompose into inner sig(65) ‖ app separator(32) ‖ contents hash(32)
    /// ‖ contents type ‖ uint16 len, and the inner signature must recover to the owner EOA over
    /// the ERC-7739 rehashed digest — which differs from the plain order signing hash.
    #[test]
    fn erc7739_signature_layout_and_recovery() {
        use k256::ecdsa::VerifyingKey;

        let (key, eoa) = parse_private_key(ANVIL_KEY).unwrap();
        let order = build_limit_order(
            U256::from(1u8),
            dec!(0.76),
            dec!(5),
            Side::Buy,
            dec!(0.01),
            DEPOSIT,
        )
        .unwrap();
        let domain = Eip712Domain {
            name: Some(ORDER_NAME.into()),
            version: Some(ORDER_VERSION.into()),
            chain_id: Some(U256::from(CHAIN_ID)),
            verifying_contract: Some(EXCHANGE_CONTRACT_NORMAL_V2),
            ..Eip712Domain::default()
        };

        let sig_hex = sign_order_erc7739(&key, &order, &domain).unwrap();
        let bytes = hex::decode(sig_hex.trim_start_matches("0x")).unwrap();

        let contents_type = Order::eip712_root_type();
        let ct = contents_type.as_bytes();
        assert_eq!(bytes.len(), 65 + 32 + 32 + ct.len() + 2);
        assert_eq!(&bytes[65..97], domain.separator().as_slice());
        assert_eq!(&bytes[97..129], order.eip712_hash_struct().as_slice());
        assert_eq!(&bytes[129..129 + ct.len()], ct);
        let suffix_len = u16::from_be_bytes([bytes[bytes.len() - 2], bytes[bytes.len() - 1]]);
        assert_eq!(usize::from(suffix_len), ct.len());

        // Independently rebuild the TypedDataSign digest the wallet contract will verify.
        let typed_data_sign_type = format!(
            "TypedDataSign(Order contents,string name,string version,uint256 chainId,\
             address verifyingContract,bytes32 salt){contents_type}"
        );
        let mut words = Vec::new();
        words.extend_from_slice(keccak256(typed_data_sign_type.as_bytes()).as_slice());
        words.extend_from_slice(order.eip712_hash_struct().as_slice());
        words.extend_from_slice(keccak256(b"DepositWallet").as_slice());
        words.extend_from_slice(keccak256(b"1").as_slice());
        words.extend_from_slice(B256::from(U256::from(CHAIN_ID)).as_slice());
        words.extend_from_slice(B256::left_padding_from(DEPOSIT.as_slice()).as_slice());
        words.extend_from_slice(B256::ZERO.as_slice());
        let mut preimage = vec![0x19u8, 0x01];
        preimage.extend_from_slice(domain.separator().as_slice());
        preimage.extend_from_slice(keccak256(&words).as_slice());
        let digest = keccak256(&preimage);
        assert_ne!(digest, order.eip712_signing_hash(&domain));

        let sig = k256::ecdsa::Signature::from_slice(&bytes[..64]).unwrap();
        let recid = k256::ecdsa::RecoveryId::from_byte(bytes[64] - 27).unwrap();
        let recovered =
            VerifyingKey::recover_from_prehash(digest.as_slice(), &sig, recid).unwrap();
        let pub_bytes = recovered.to_encoded_point(false);
        let rec_addr = Address::from_slice(&keccak256(&pub_bytes.as_bytes()[1..])[12..]);
        assert_eq!(rec_addr, eoa);
    }

    /// The ClobAuth digest must move with every field it claims to bind.
    #[test]
    fn clob_auth_digest_binds_account_and_time() {
        let d1 = clob_auth_digest(DEPOSIT, "1751800000", 0);
        assert_ne!(d1, clob_auth_digest(DEPOSIT, "1751800001", 0));
        assert_ne!(d1, clob_auth_digest(Address::ZERO, "1751800000", 0));
        assert_ne!(d1, clob_auth_digest(DEPOSIT, "1751800000", 1));
    }

    /// The wire body must match the V2 API shape: salt as a number, metadata/builder as 0x hex,
    /// unsigned expiration inside the order, and deferExec at the top level.
    #[test]
    fn payload_serializes_to_v2_shape() {
        let order = build_limit_order(
            U256::from(1u8),
            dec!(0.76),
            dec!(5),
            Side::Buy,
            dec!(0.01),
            DEPOSIT,
        )
        .unwrap();
        let payload = SignedOrderPayload {
            order,
            expiration: U256::ZERO,
            signature_hex: "0xabc".to_string(),
            order_type: PolyOrderType::Fak,
            owner: Uuid::nil(),
            post_only: false,
            defer_exec: false,
        };
        let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&payload).unwrap())
            .unwrap();
        assert!(v["order"]["salt"].is_u64());
        assert_eq!(v["order"]["side"], "BUY");
        assert_eq!(
            v["order"]["metadata"],
            format!("0x{}", "0".repeat(64))
        );
        assert_eq!(v["order"]["builder"], format!("0x{}", "0".repeat(64)));
        assert_eq!(v["order"]["expiration"], "0");
        assert!(v["order"]["timestamp"].is_string());
        assert_eq!(v["orderType"], "FAK");
        assert_eq!(v["postOnly"], false);
        assert_eq!(v["deferExec"], false);
        // V1-only fields must be gone from the wire format.
        assert!(v["order"].get("taker").is_none());
        assert!(v["order"].get("nonce").is_none());
        assert!(v["order"].get("feeRateBps").is_none());
    }

    /// The four `orderType` strings are recomputed by nothing — they are what the CLOB matches
    /// on, so a typo silently changes an order's behaviour rather than failing.
    #[test]
    fn order_type_wire_values_and_flags() {
        assert_eq!(PolyOrderType::Fak.wire(), "FAK");
        assert_eq!(PolyOrderType::Fok.wire(), "FOK");
        assert_eq!(PolyOrderType::Gtc { post_only: false }.wire(), "GTC");
        assert_eq!(
            PolyOrderType::Gtd { lifetime_secs: 300, post_only: false }.wire(),
            "GTD"
        );

        // Only the resting types can rest, and only they can be post-only.
        assert!(!PolyOrderType::Fak.rests() && !PolyOrderType::Fok.rests());
        assert!(PolyOrderType::Gtc { post_only: false }.rests());
        assert!(PolyOrderType::Gtd { lifetime_secs: 300, post_only: true }.rests());
        assert!(!PolyOrderType::Fak.post_only());
        assert!(PolyOrderType::Gtc { post_only: true }.post_only());
        assert!(PolyOrderType::Gtd { lifetime_secs: 300, post_only: true }.post_only());
    }

    /// GTD is the only type with an expiration, and the venue kills the order a minute before the
    /// timestamp it is given — so the wire value must carry that minute on top of the lifetime the
    /// caller asked for, or every GTD order would live 60 s less than requested.
    #[test]
    fn gtd_expiration_carries_the_security_buffer() {
        let now = 1_800_000_000i64;
        assert_eq!(PolyOrderType::Fak.expiration(now).unwrap(), U256::ZERO);
        assert_eq!(PolyOrderType::Fok.expiration(now).unwrap(), U256::ZERO);
        assert_eq!(
            PolyOrderType::Gtc { post_only: true }.expiration(now).unwrap(),
            U256::ZERO
        );
        assert_eq!(
            PolyOrderType::Gtd { lifetime_secs: 300, post_only: false }
                .expiration(now)
                .unwrap(),
            U256::from(1_800_000_000u64 + 60 + 300)
        );
        // Below the floor the CLOB accepts: refused here rather than sent to be refused there.
        assert!(PolyOrderType::Gtd { lifetime_secs: 119, post_only: false }
            .expiration(now)
            .is_err());
        assert!(PolyOrderType::Gtd { lifetime_secs: 120, post_only: false }
            .expiration(now)
            .is_ok());
    }

    /// A GTD post-only order's wire body: the type string, a non-zero expiration *inside* the
    /// order object, and postOnly at the top level beside it.
    #[test]
    fn gtd_post_only_payload_shape() {
        let order = build_limit_order(
            U256::from(1u8),
            dec!(0.76),
            dec!(5),
            Side::Buy,
            dec!(0.01),
            DEPOSIT,
        )
        .unwrap();
        let order_type = PolyOrderType::Gtd { lifetime_secs: 600, post_only: true };
        let payload = SignedOrderPayload {
            order,
            expiration: order_type.expiration(1_800_000_000).unwrap(),
            signature_hex: "0xabc".to_string(),
            order_type,
            owner: Uuid::nil(),
            post_only: order_type.post_only(),
            defer_exec: false,
        };
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&payload).unwrap()).unwrap();
        assert_eq!(v["orderType"], "GTD");
        assert_eq!(v["postOnly"], true);
        assert_eq!(v["order"]["expiration"], (1_800_000_000u64 + 60 + 600).to_string());
    }

    /// Ack amounts arrive as plain-decimal strings (a live BUY capture; see
    /// sample_response_poly.jsonc) and must parse to f64 — but never fail the whole ack: an
    /// accepted order with a mis-shaped amount reported as Err would leave its fill untracked.
    #[test]
    fn order_ack_amounts_parse_forgivingly() {
        let live = r#"{
            "errorMsg": "",
            "orderID": "0xd0b4",
            "takingAmount": "5",
            "makingAmount": "1.61",
            "status": "matched",
            "transactionsHashes": ["0x0d0a"],
            "success": true
        }"#;
        let ack: PolyOrderAck = serde_json::from_str(live).unwrap();
        assert!(ack.success);
        assert_eq!(ack.making_amount, Some(1.61));
        assert_eq!(ack.taking_amount, Some(5.0));
        // Shares are the taking side of a BUY, the making side of a SELL.
        assert_eq!(ack.filled_shares(Side::Buy), Some(5.0));
        assert_eq!(ack.filled_shares(Side::Sell), Some(1.61));

        // Absent or unparseable amounts degrade to None; success/status still come through.
        let odd = r#"{"success": true, "status": "matched", "makingAmount": ""}"#;
        let ack: PolyOrderAck = serde_json::from_str(odd).unwrap();
        assert!(ack.success);
        assert_eq!(ack.making_amount, None);
        assert_eq!(ack.taking_amount, None);
    }
}
