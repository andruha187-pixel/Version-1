import os
import json
import time
import math
import sqlite3
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# MULTI7 F/G/H/J PAPER BOT — FIRST-V2 CROSS-TOKEN CONSENSUS + PROFIT-PROTECT STOP
# ============================================================
# Common first-V2 vote:
#   * first eligible signal per token/market
#   * price 0.55..0.75, momentum 0.03..0.30
#   * other tokens can confirm a target within the previous 10 seconds
#
# F: target 0.67..0.70 + >=1 other same-side first-V2 vote; ENTRY 5
# G: target 0.67..0.70 + >=2 other same-side first-V2 votes; ENTRY 5
# H: G + one safer reversal DCA 5
# J: target 0.67..0.75 + >=2 other same-side first-V2 votes; ENTRY 5
#
# All target momentum 0.05..0.10. No switching.
# Profit protection: arm after best bid rises +0.05 from weighted gross entry avg;
# then exit if best bid falls to the fee-adjusted level that targets at least +$0.10 total PnL.
# PAPER ONLY.
# ============================================================

VERSION = "16.2-paper-multi7-consensus-fghj-profit10"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))

# BTC plus our six additional Polymarket 5-minute crypto chains.
# Hyperliquid uses the HYPE ticker/slug.
ASSET_CONFIG = {
    "BTC":  {"prefix": "btc-updown-5m",  "label": "Bitcoin"},
    "XRP":  {"prefix": "xrp-updown-5m",  "label": "XRP"},
    "BNB":  {"prefix": "bnb-updown-5m",  "label": "BNB"},
    "SOL":  {"prefix": "sol-updown-5m",  "label": "Solana"},
    "ETH":  {"prefix": "eth-updown-5m",  "label": "Ethereum"},
    "DOGE": {"prefix": "doge-updown-5m", "label": "Dogecoin"},
    "HYPE": {"prefix": "hype-updown-5m", "label": "Hyperliquid"},
}

def _configured_symbols():
    raw = os.getenv("SYMBOLS", "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE")
    out = []
    for item in raw.split(","):
        sym = item.strip().upper()
        if sym in ASSET_CONFIG and sym not in out:
            out.append(sym)
    return out or ["BTC", "XRP", "BNB", "SOL", "ETH", "DOGE", "HYPE"]

SYMBOLS = _configured_symbols()

DECISION_INTERVAL = float(os.getenv("DECISION_INTERVAL", "3.0"))
TRADE_WINDOW_SECONDS = int(os.getenv("TRADE_WINDOW_SECONDS", "180"))
ENTRY_ORDER_SIZE = float(os.getenv("ENTRY_ORDER_SIZE", "5"))
DCA_ORDER_SIZE = float(os.getenv("DCA_ORDER_SIZE", "5"))
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))
BREAKEVEN_STOP_ENABLED = os.getenv("BREAKEVEN_STOP_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
BREAKEVEN_TRIGGER_MOVE = float(os.getenv("BREAKEVEN_TRIGGER_MOVE", "0.05"))
BREAKEVEN_MIN_PROFIT_USDC = float(os.getenv("BREAKEVEN_MIN_PROFIT_USDC", "0.10"))
DISCOVERY_INTERVAL = float(os.getenv("DISCOVERY_INTERVAL", "10"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))

MEMORY_CLEANUP_INTERVAL = int(os.getenv("MEMORY_CLEANUP_INTERVAL", "60"))
MEMORY_KEEP_RESOLVED_SEC = int(os.getenv("MEMORY_KEEP_RESOLVED_SEC", "900"))
WS_MAX_CONNECTION_AGE_SEC = int(os.getenv("WS_MAX_CONNECTION_AGE_SEC", "900"))
MEMORY_LOG_INTERVAL = int(os.getenv("MEMORY_LOG_INTERVAL", "300"))

ENTRY_MOVE = float(os.getenv("ENTRY_MOVE", "0.03"))
LOOKBACK_TICKS = int(os.getenv("LOOKBACK_TICKS", "2"))

V2_ELIGIBLE_PRICE_MIN = float(os.getenv("V2_ELIGIBLE_PRICE_MIN", "0.55"))
V2_ELIGIBLE_PRICE_MAX = float(os.getenv("V2_ELIGIBLE_PRICE_MAX", "0.75"))
V2_ELIGIBLE_MOM_MIN = float(os.getenv("V2_ELIGIBLE_MOM_MIN", "0.03"))
V2_ELIGIBLE_MOM_MAX = float(os.getenv("V2_ELIGIBLE_MOM_MAX", "0.30"))

SAFE_ENTRY_PRICE_MIN = float(os.getenv("SAFE_ENTRY_PRICE_MIN", "0.67"))
SAFE_ENTRY_PRICE_MAX = float(os.getenv("SAFE_ENTRY_PRICE_MAX", "0.75"))
SAFE_ENTRY_MOM_MIN = float(os.getenv("SAFE_ENTRY_MOM_MIN", "0.05"))
SAFE_ENTRY_MOM_MAX = float(os.getenv("SAFE_ENTRY_MOM_MAX", "0.10"))

MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))

# Reversal-DCA experiment — same settings for every token.
DCA_ARM_PRICE = float(os.getenv("DCA_ARM_PRICE", "0.50"))
DCA_MAX_BUY_PRICE = float(os.getenv("DCA_MAX_BUY_PRICE", "0.60"))
DCA_REBOUND_MOM = float(os.getenv("DCA_REBOUND_MOM", "0.05"))
DCA_DEADLINE_SEC = float(os.getenv("DCA_DEADLINE_SEC", "120"))

# C — tighter entry + safer reversal DCA.
C_SAFE_ENTRY_PRICE_MIN = float(os.getenv("C_SAFE_ENTRY_PRICE_MIN", "0.67"))
C_SAFE_ENTRY_PRICE_MAX = float(os.getenv("C_SAFE_ENTRY_PRICE_MAX", "0.70"))
C_DCA_MIN_BUY_PRICE = float(os.getenv("C_DCA_MIN_BUY_PRICE", "0.30"))
C_DCA_MAX_BUY_PRICE = float(os.getenv("C_DCA_MAX_BUY_PRICE", "0.60"))
C_DCA_REBOUND_MOM_MIN = float(os.getenv("C_DCA_REBOUND_MOM_MIN", "0.05"))
C_DCA_REBOUND_MOM_MAX = float(os.getenv("C_DCA_REBOUND_MOM_MAX", "0.15"))

# F/G/H/J — cross-token first-V2 consensus experiment.
CONSENSUS_WINDOW_SEC = float(os.getenv("CONSENSUS_WINDOW_SEC", "10"))
F_CONSENSUS_MIN_OTHER_TOKENS = int(os.getenv("F_CONSENSUS_MIN_OTHER_TOKENS", "1"))
G_CONSENSUS_MIN_OTHER_TOKENS = int(os.getenv("G_CONSENSUS_MIN_OTHER_TOKENS", "2"))
H_CONSENSUS_MIN_OTHER_TOKENS = int(os.getenv("H_CONSENSUS_MIN_OTHER_TOKENS", "2"))
J_CONSENSUS_MIN_OTHER_TOKENS = int(os.getenv("J_CONSENSUS_MIN_OTHER_TOKENS", "2"))


def _strategy_set(symbol):
    common = {
        "symbol": symbol,
        "entry_move": ENTRY_MOVE,
        "lookback": LOOKBACK_TICKS,
        "v2_price_min": V2_ELIGIBLE_PRICE_MIN,
        "v2_price_max": V2_ELIGIBLE_PRICE_MAX,
        "v2_mom_min": V2_ELIGIBLE_MOM_MIN,
        "v2_mom_max": V2_ELIGIBLE_MOM_MAX,
        "safe_entry_mom_min": SAFE_ENTRY_MOM_MIN,
        "safe_entry_mom_max": SAFE_ENTRY_MOM_MAX,
        "breakeven_stop_enabled": BREAKEVEN_STOP_ENABLED,
        "consensus_enabled": True,
        "consensus_window_sec": CONSENSUS_WINDOW_SEC,
        "consensus_source": "FIRST_V2_ELIGIBLE",
    }

    f = dict(common)
    f.update({
        "code": "F",
        "name": f"{symbol}_F_TIGHT_ONE_V2",
        "short": f"{symbol} / F TIGHT + 1 V2 CONFIRM",
        "safe_entry_price_min": C_SAFE_ENTRY_PRICE_MIN,
        "safe_entry_price_max": C_SAFE_ENTRY_PRICE_MAX,
        "consensus_min_other_tokens": F_CONSENSUS_MIN_OTHER_TOKENS,
        "max_buys_side": 1,
        "dca_enabled": False,
    })

    g = dict(common)
    g.update({
        "code": "G",
        "name": f"{symbol}_G_TIGHT_TWO_V2",
        "short": f"{symbol} / G TIGHT + 2 V2 CONFIRM",
        "safe_entry_price_min": C_SAFE_ENTRY_PRICE_MIN,
        "safe_entry_price_max": C_SAFE_ENTRY_PRICE_MAX,
        "consensus_min_other_tokens": G_CONSENSUS_MIN_OTHER_TOKENS,
        "max_buys_side": 1,
        "dca_enabled": False,
    })

    h = dict(common)
    h.update({
        "code": "H",
        "name": f"{symbol}_H_TIGHT_TWO_V2_SAFE_DCA",
        "short": f"{symbol} / H G + SAFE DCA 5+5",
        "safe_entry_price_min": C_SAFE_ENTRY_PRICE_MIN,
        "safe_entry_price_max": C_SAFE_ENTRY_PRICE_MAX,
        "consensus_min_other_tokens": H_CONSENSUS_MIN_OTHER_TOKENS,
        "max_buys_side": 2,
        "dca_enabled": True,
        "dca_arm_price": DCA_ARM_PRICE,
        "dca_min_buy_price": C_DCA_MIN_BUY_PRICE,
        "dca_max_buy_price": C_DCA_MAX_BUY_PRICE,
        "dca_rebound_mom": C_DCA_REBOUND_MOM_MIN,
        "dca_rebound_mom_max": C_DCA_REBOUND_MOM_MAX,
        "dca_deadline_sec": DCA_DEADLINE_SEC,
    })

    j = dict(common)
    j.update({
        "code": "J",
        "name": f"{symbol}_J_WIDE_TWO_V2",
        "short": f"{symbol} / J WIDE + 2 V2 CONFIRM",
        "safe_entry_price_min": SAFE_ENTRY_PRICE_MIN,
        "safe_entry_price_max": SAFE_ENTRY_PRICE_MAX,
        "consensus_min_other_tokens": J_CONSENSUS_MIN_OTHER_TOKENS,
        "max_buys_side": 1,
        "dca_enabled": False,
    })
    return [f, g, h, j]


STRATEGIES = [s for symbol in SYMBOLS for s in _strategy_set(symbol)]
STRATEGIES_BY_SYMBOL = {
    symbol: [s for s in STRATEGIES if s["symbol"] == symbol]
    for symbol in SYMBOLS
}
STRATEGY_BY_NAME = {x["name"]: x for x in STRATEGIES}

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    probe = DATA_DIR / ".write_test"
    probe.write_text("ok")
    probe.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "safe67_multi7_consensus_fghj_profit10.db"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("safe67-multi7-consensus-fghj-be")

session: Optional[aiohttp.ClientSession] = None

books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()
price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=100)))
strategy_state = {}
settle_lock = asyncio.Lock()


# ============================================================
# HELPERS
# ============================================================

def now_ts():
    return int(time.time())


def now_ms():
    return int(time.time() * 1000)


def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def sf(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def si(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def jd(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def parse_jsonish(v):
    if isinstance(v, list):
        return v
    if v is None:
        return []
    try:
        x = json.loads(v)
        return x if isinstance(x, list) else []
    except Exception:
        return []


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def fee_usdc(shares, price):
    fee = shares * CRYPTO_FEE_RATE * price * (1.0 - price)
    return round(fee, 5) if fee >= 0.000005 else 0.0



# ============================================================
# DATABASE / PERSISTENT PAPER ACCOUNTS
# ============================================================

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS discovered_markets (
            condition_id TEXT PRIMARY KEY,
            symbol TEXT,
            question TEXT,
            slug TEXT,
            start_ts INTEGER,
            end_ts INTEGER,
            up_asset TEXT,
            down_asset TEXT,
            discovered_ms INTEGER,
            resolved INTEGER DEFAULT 0,
            winning_asset TEXT,
            winning_outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS gate_decisions (
            condition_id TEXT,
            variant TEXT,
            decision_ms INTEGER,
            elapsed_sec REAL,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            passed INTEGER,
            reason TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            signal_type TEXT,
            elapsed_sec REAL
        );

        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            signal_type TEXT,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_cost REAL,
            fee REAL,
            total_cost REAL,
            book_age_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS dca_events (
            condition_id TEXT,
            variant TEXT,
            armed_ms INTEGER,
            armed_elapsed_sec REAL,
            armed_ask REAL,
            filled_ms INTEGER,
            filled_elapsed_sec REAL,
            filled_ask REAL,
            filled_momentum REAL,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS v2_votes (
            condition_id TEXT PRIMARY KEY,
            symbol TEXT,
            decision_ms INTEGER,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            elapsed_sec REAL
        );

        CREATE TABLE IF NOT EXISTS consensus_events (
            condition_id TEXT,
            variant TEXT,
            decision_ms INTEGER,
            target_symbol TEXT,
            target_outcome TEXT,
            target_ask REAL,
            target_momentum REAL,
            window_sec REAL,
            required_count INTEGER,
            confirm_count INTEGER,
            confirm_symbols_json TEXT,
            confirm_ages_ms_json TEXT,
            passed INTEGER,
            reason TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS breakeven_events (
            condition_id TEXT,
            variant TEXT,
            armed_ms INTEGER,
            arm_bid REAL,
            entry_avg_price REAL,
            arm_trigger_price REAL,
            stop_price REAL,
            triggered_ms INTEGER,
            trigger_bid REAL,
            completed_ms INTEGER,
            exit_filled_shares REAL DEFAULT 0,
            exit_avg_price REAL,
            exit_fee REAL DEFAULT 0,
            exit_net REAL DEFAULT 0,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS stop_events (
            condition_id TEXT,
            variant TEXT,
            trigger_ms INTEGER,
            trigger_bid REAL,
            stop_price REAL,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS paper_exits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exit_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            reason TEXT,
            trigger_price REAL,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_proceeds REAL,
            fee REAL,
            net_proceeds REAL,
            book_age_ms INTEGER,
            book_received_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS market_results (
            condition_id TEXT,
            variant TEXT,
            winning_asset TEXT,
            winning_outcome TEXT,
            buy_cost REAL,
            exit_proceeds REAL,
            payout REAL,
            pnl REAL,
            buy_trades INTEGER,
            exit_trades INTEGER,
            up_bought REAL,
            down_bought REAL,
            up_exited REAL,
            down_exited REAL,
            stopped_out INTEGER,
            settled_ms INTEGER,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS position_trajectory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            elapsed_sec REAL,
            primary_asset TEXT,
            primary_outcome TEXT,
            opposite_asset TEXT,
            bought_shares REAL,
            exited_shares REAL,
            remaining_shares REAL,
            gross_entry_cost REAL,
            entry_fees REAL,
            total_buy_cost REAL,
            exit_net_so_far REAL,
            primary_best_bid REAL,
            primary_best_ask REAL,
            opposite_best_bid REAL,
            opposite_best_ask REAL,
            mark_filled_shares REAL,
            mark_avg_price REAL,
            mark_fee REAL,
            mark_net_proceeds REAL,
            unrealized_total_pnl REAL,
            mfe_pnl REAL,
            mae_pnl REAL,
            stop_triggered INTEGER
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_gate_ms ON gate_decisions(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_ms ON paper_trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_exits_ms ON paper_exits(exit_ms);
        CREATE INDEX IF NOT EXISTS idx_be_armed_ms ON breakeven_events(armed_ms);
        CREATE INDEX IF NOT EXISTS idx_dca_armed_ms ON dca_events(armed_ms);
        CREATE INDEX IF NOT EXISTS idx_consensus_ms ON consensus_events(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_v2_votes_ms ON v2_votes(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_results_ms ON market_results(settled_ms);
        CREATE INDEX IF NOT EXISTS idx_traj_ms ON position_trajectory(sample_ms);
        CREATE INDEX IF NOT EXISTS idx_traj_cond ON position_trajectory(condition_id,variant,sample_ms);
        """)

        defaults = {"trading_enabled": "0"}
        for strategy in STRATEGIES:
            defaults[f"paper_initial:{strategy['name']}"] = str(PAPER_START_BALANCE)
            defaults[f"paper_cash:{strategy['name']}"] = str(PAPER_START_BALANCE)
        for key, value in defaults.items():
            if conn.execute("SELECT 1 FROM state WHERE key=?", (key,)).fetchone() is None:
                conn.execute("INSERT INTO state(key,value) VALUES(?,?)", (key, value))
        conn.commit()


def state_get(key, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def state_set(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        conn.commit()


def paper_cash(strategy_name):
    return sf(
        state_get(f"paper_cash:{strategy_name}", PAPER_START_BALANCE),
        PAPER_START_BALANCE,
    )


def paper_initial(strategy_name):
    return sf(
        state_get(f"paper_initial:{strategy_name}", PAPER_START_BALANCE),
        PAPER_START_BALANCE,
    )


def set_paper_cash(strategy_name, value):
    state_set(f"paper_cash:{strategy_name}", round(float(value), 10))


def trading_enabled():
    return state_get("trading_enabled", "0") == "1"


# ============================================================
# HTTP / BOOK
# ============================================================

async def get_json(url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                text = await r.text()
                if r.status == 200:
                    return json.loads(text)
                log.warning("HTTP %s %s %s -> %s", r.status, url, params, text[:200])
        except Exception as e:
            log.warning("GET %s failed: %s", url, e)
        await asyncio.sleep(0.3 * (attempt + 1))
    return None


def level_map(rows):
    out = {}
    for x in rows or []:
        if not isinstance(x, dict):
            continue
        p = sf(x.get("price"), math.nan)
        q = sf(x.get("size"), 0)
        if not math.isnan(p) and q > 0:
            out[p] = q
    return out


def apply_book(asset, payload, source="ws"):
    books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": now_ms(),
        "source": source,
    }


def apply_price_change(payload):
    changes = payload.get("price_changes") or payload.get("priceChanges") or []
    recv = now_ms()
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        asset = str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not asset:
            continue
        b = books.setdefault(asset, {
            "bids": {}, "asks": {}, "received_ms": recv, "source": "ws-delta"
        })
        p = sf(ch.get("price"), math.nan)
        q = sf(ch.get("size"), 0)
        side = str(ch.get("side", "")).upper()
        if math.isnan(p):
            continue
        target = b["bids"] if side == "BUY" else b["asks"]
        if q <= 0:
            target.pop(p, None)
        else:
            target[p] = q
        b["received_ms"] = recv
        b["source"] = "ws"


def best_ask(asset):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return None
    return min(b["asks"])


def best_bid(asset):
    b = books.get(asset)
    if not b or not b.get("bids"):
        return None
    return max(b["bids"])


async def refresh_book(asset):
    data = await get_json(f"{CLOB_API}/book", params={"token_id": asset})
    if isinstance(data, dict):
        apply_book(asset, data, "rest")
        return True
    return False


async def ensure_book(asset):
    b = books.get(asset)
    if b and b.get("asks"):
        age = now_ms() - b["received_ms"]
        if age <= MAX_BOOK_AGE_MS:
            return age
    await refresh_book(asset)
    b = books.get(asset)
    if not b:
        return None
    return now_ms() - b["received_ms"]


def simulate_buy(asset, wanted):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return [], 0.0
    remaining = wanted
    fills = []
    for p in sorted(b["asks"]):
        q = b["asks"][p]
        take = min(q, remaining)
        if take > 0:
            fills.append((p, take))
            remaining -= take
        if remaining <= 1e-12:
            break
    return fills, wanted - remaining


def simulate_sell(asset, wanted):
    """Walk visible bids from best to worst for an executable PAPER exit mark."""
    b = books.get(asset)
    if not b or not b.get("bids"):
        return [], 0.0
    remaining = wanted
    fills = []
    for p in sorted(b["bids"], reverse=True):
        q = b["bids"][p]
        take = min(q, remaining)
        if take > 0:
            fills.append((p, take))
            remaining -= take
        if remaining <= 1e-12:
            break
    return fills, wanted - remaining


# ============================================================
# MARKET DISCOVERY
# ============================================================

def market_symbol(market):
    sym = str((market or {}).get("symbol") or "").upper()
    if sym in ASSET_CONFIG:
        return sym
    slug = str((market or {}).get("slug") or "").lower()
    for candidate, cfg in ASSET_CONFIG.items():
        if slug.startswith(cfg["prefix"] + "-"):
            return candidate
    return None


def strategies_for_market(market):
    return STRATEGIES_BY_SYMBOL.get(market_symbol(market), [])


def slot_start_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None


async def fetch_event_by_slug(slug):
    for url, params in (
        (f"{GAMMA_API}/events/slug/{slug}", None),
        (f"{GAMMA_API}/events", {"slug": slug}),
    ):
        data = await get_json(url, params=params)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
    return None


def parse_market_from_event(raw, event, symbol):
    if not isinstance(raw, dict) or symbol not in ASSET_CONFIG:
        return None
    cid = str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not cid:
        return None
    title = str(raw.get("question") or raw.get("title") or event.get("title") or "Unknown")
    slug = str(raw.get("slug") or event.get("slug") or "")
    expected_prefix = ASSET_CONFIG[symbol]["prefix"] + "-"
    if slug and not slug.lower().startswith(expected_prefix):
        return None

    outcomes = [str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    if len(tokens) < 2:
        return None

    up_asset = down_asset = None
    for i, outcome in enumerate(outcomes):
        if i >= len(tokens):
            break
        if outcome in {"UP", "YES"}:
            up_asset = tokens[i]
        elif outcome in {"DOWN", "NO"}:
            down_asset = tokens[i]
    up_asset = up_asset or tokens[0]
    down_asset = down_asset or tokens[1]

    start_ts = slot_start_from_slug(slug)
    if not start_ts:
        start_dt = parse_iso(raw.get("startDate")) or parse_iso(event.get("startDate"))
        start_ts = int(start_dt.timestamp()) if start_dt else None
    if not start_ts:
        return None

    return {
        "condition_id": cid,
        "symbol": symbol,
        "question": title,
        "slug": slug,
        "start_ts": int(start_ts),
        "end_ts": int(start_ts) + 300,
        "up_asset": str(up_asset),
        "down_asset": str(down_asset),
        "raw": raw,
    }


async def discover_slot_market(symbol, slot_start):
    cfg = ASSET_CONFIG.get(symbol)
    if not cfg:
        return None
    slug = f"{cfg['prefix']}-{slot_start}"
    event = await fetch_event_by_slug(slug)
    if not event or not isinstance(event.get("markets"), list):
        return None
    for raw in event["markets"]:
        market = parse_market_from_event(raw, event, symbol)
        if market:
            return market
    return None


def persist_market(m):
    with db() as conn:
        conn.execute("""
            INSERT INTO discovered_markets(
                condition_id,symbol,question,slug,start_ts,end_ts,up_asset,down_asset,discovered_ms
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                symbol=excluded.symbol, question=excluded.question, slug=excluded.slug,
                start_ts=excluded.start_ts, end_ts=excluded.end_ts,
                up_asset=excluded.up_asset, down_asset=excluded.down_asset
        """, (
            m["condition_id"], market_symbol(m), m["question"], m["slug"],
            m["start_ts"], m["end_ts"], m["up_asset"], m["down_asset"], now_ms(),
        ))
        conn.commit()


async def subscribe_asset(asset):
    if not asset or asset in subscribed_assets:
        return
    subscribed_assets.add(asset)
    await ws_send_queue.put({"operation": "subscribe", "assets_ids": [asset]})


async def discovery_loop():
    last_current_slot = {}
    while True:
        try:
            n = now_ts()
            current = (n // 300) * 300
            for symbol in SYMBOLS:
                candidates = []
                for slot_start in (current, current + 300, current - 300):
                    market = await discover_slot_market(symbol, slot_start)
                    if market:
                        candidates.append(market)

                if not candidates:
                    log.info("Discovery %s: market not found for slot %s", symbol, utc_iso(current))
                    continue

                active = [m for m in candidates if m["start_ts"] - 5 <= n <= m["end_ts"] + 5]
                chosen = min(active or candidates, key=lambda m: abs(n - m["start_ts"]))
                for market in candidates:
                    cid = market["condition_id"]
                    if cid in markets:
                        continue
                    markets[cid] = market
                    persist_market(market)
                    await subscribe_asset(market["up_asset"])
                    await subscribe_asset(market["down_asset"])
                    log.info(
                        "MARKET %s | %s | slug=%s | start=%s",
                        symbol, market["question"], market["slug"], utc_iso(market["start_ts"]),
                    )
                if last_current_slot.get(symbol) != current:
                    log.info("CURRENT %s %s | selected=%s", symbol, utc_iso(current), chosen["slug"])
                    last_current_slot[symbol] = current
        except Exception:
            log.exception("Discovery loop failed")
        await asyncio.sleep(DISCOVERY_INTERVAL)

# ============================================================
# POLYMARKET WEBSOCKET
# ============================================================

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("", "PING", "PONG"):
        return []
    try:
        x = json.loads(raw)
        return x if isinstance(x, list) else [x]
    except Exception:
        return []


async def ws_sender(ws):
    while True:
        msg = await ws_send_queue.get()
        try:
            await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg)
            return


async def ws_ping(ws):
    while True:
        try:
            await ws.send("PING")
        except Exception:
            return
        await asyncio.sleep(10)


async def ws_loop():
    while True:
        try:
            if not subscribed_assets:
                await asyncio.sleep(1)
                continue

            async with websockets.connect(
                MARKET_WS,
                ping_interval=None,
                close_timeout=5,
                max_size=20_000_000,
            ) as ws:
                await ws.send(jd({
                    "assets_ids": list(subscribed_assets),
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log.info("WS connected | assets=%d", len(subscribed_assets))

                sender = asyncio.create_task(ws_sender(ws))
                ping = asyncio.create_task(ws_ping(ws))
                try:
                    ws_started = time.monotonic()
                    async for raw in ws:
                        if time.monotonic() - ws_started >= WS_MAX_CONNECTION_AGE_SEC:
                            log.info("WS periodic reconnect | active_assets=%d", len(subscribed_assets))
                            break
                        for ev in parse_ws(raw):
                            if not isinstance(ev, dict):
                                continue
                            et = str(ev.get("event_type") or ev.get("type") or "")
                            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev
                            if et == "book":
                                asset = str(payload.get("asset_id") or payload.get("token_id") or "")
                                if asset:
                                    apply_book(asset, payload)
                            elif et == "price_change":
                                apply_price_change(payload)
                            elif et == "market_resolved":
                                await settle_from_resolution(payload)
                finally:
                    sender.cancel()
                    ping.cancel()
        except Exception as e:
            log.warning("WS reconnect: %s", e)
            await asyncio.sleep(1)


# ============================================================
# MEMORY / RENDER STABILITY
# ============================================================

def current_rss_mb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return None


def cleanup_resolved_market_memory():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    with db() as conn:
        rows = conn.execute(
            "SELECT condition_id FROM discovered_markets WHERE resolved=1 AND end_ts < ?",
            (cutoff,),
        ).fetchall()
    old_cids = {str(r["condition_id"]) for r in rows}
    if not old_cids:
        return 0

    for cid in old_cids:
        markets.pop(cid, None)
        price_history.pop(cid, None)
    for key in list(strategy_state):
        if key[0] in old_cids:
            strategy_state.pop(key, None)

    keep_assets = set()
    for m in markets.values():
        if m.get("up_asset"):
            keep_assets.add(str(m["up_asset"]))
        if m.get("down_asset"):
            keep_assets.add(str(m["down_asset"]))
    for asset in list(books):
        if asset not in keep_assets:
            books.pop(asset, None)
    subscribed_assets.intersection_update(keep_assets)
    return len(old_cids)


async def memory_maintenance_loop():
    last_mem_log = 0.0
    while True:
        try:
            removed = cleanup_resolved_market_memory()
            mono = time.monotonic()
            if removed or mono - last_mem_log >= MEMORY_LOG_INTERVAL:
                rss = current_rss_mb()
                log.info(
                    "MEMORY | RSS=%s | removed_markets=%d | markets=%d | books=%d | state=%d | assets=%d",
                    f"{rss:.1f} MB" if rss is not None else "n/a",
                    removed, len(markets), len(books), len(strategy_state), len(subscribed_assets),
                )
                last_mem_log = mono
        except Exception:
            log.exception("Memory maintenance failed")
        await asyncio.sleep(MEMORY_CLEANUP_INTERVAL)




# ============================================================
# A/B STRATEGY ENGINE
# ============================================================

def get_variant_state(condition, variant):
    key = (condition, variant["name"])
    if key in strategy_state:
        return strategy_state[key]

    st = {
        "buys": defaultdict(int),
        "last_buy": {},
        "started_sides": set(),
        "primary_asset": None,
        "gate_decided": False,
        "gate_passed": False,
        "gate_asset": None,
        "stopped_out": False,
        "dca_armed": False,
        "dca_armed_ms": None,
        "dca_armed_ask": None,
    }

    # Hydrate from DB so a Render restart cannot duplicate an open PAPER entry.
    with db() as conn:
        gate = conn.execute(
            "SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()
        if gate:
            st["gate_decided"] = True
            st["gate_passed"] = bool(gate["passed"])
            st["gate_asset"] = str(gate["asset"]) if gate["passed"] else None

        rows = conn.execute(
            "SELECT * FROM paper_trades WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant["name"]),
        ).fetchall()
        for r in rows:
            asset = str(r["asset"])
            st["buys"][asset] += 1
            st["last_buy"][asset] = sf(r["avg_price"])
            st["started_sides"].add(asset)
            if st["primary_asset"] is None:
                st["primary_asset"] = asset

        dca = conn.execute(
            "SELECT * FROM dca_events WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()
        if dca:
            st["dca_armed"] = True
            st["dca_armed_ms"] = si(dca["armed_ms"])
            st["dca_armed_ask"] = sf(dca["armed_ask"])

        if conn.execute(
            "SELECT 1 FROM stop_events WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone():
            st["stopped_out"] = True

    strategy_state[key] = st
    return st


def momentum_for(condition, asset, lookback):
    h = price_history[condition][asset]
    if len(h) <= lookback:
        return None, None
    current = h[-1][1]
    ref = h[-1 - lookback][1]
    return current - ref, ref


def store_gate_decision(condition, variant, asset, outcome, ask, ref, mom, elapsed, passed, reason):
    with db() as conn:
        conn.execute("""
            INSERT INTO gate_decisions(
                condition_id,variant,decision_ms,elapsed_sec,asset,outcome,ask,
                reference_ask,momentum,passed,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (
            condition, variant["name"], now_ms(), elapsed, asset, outcome, ask,
            ref, mom, 1 if passed else 0, reason,
        ))
        conn.commit()


def store_signal(condition, variant, asset, outcome, ask, ref, mom, signal_type, elapsed):
    with db() as conn:
        conn.execute("""
            INSERT INTO signals(
                signal_ms,condition_id,variant,asset,outcome,ask,
                reference_ask,momentum,signal_type,elapsed_sec
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, variant["name"], asset, outcome,
            ask, ref, mom, signal_type, elapsed,
        ))
        conn.commit()


def arm_dca(condition, variant, ask, elapsed):
    with db() as conn:
        conn.execute("""
            INSERT INTO dca_events(
                condition_id,variant,armed_ms,armed_elapsed_sec,armed_ask
            ) VALUES(?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (condition, variant["name"], now_ms(), elapsed, ask))
        conn.commit()
    st = get_variant_state(condition, variant)
    st["dca_armed"] = True
    if st.get("dca_armed_ms") is None:
        st["dca_armed_ms"] = now_ms()
        st["dca_armed_ask"] = ask


def mark_dca_filled(condition, variant, ask, mom, elapsed):
    with db() as conn:
        conn.execute("""
            UPDATE dca_events
            SET filled_ms=?,filled_elapsed_sec=?,filled_ask=?,filled_momentum=?
            WHERE condition_id=? AND variant=?
        """, (now_ms(), elapsed, ask, mom, condition, variant["name"]))
        conn.commit()


def trim_fills_to_budget(fills, max_total):
    if max_total <= 0:
        return [], 0.0
    out, spent, shares = [], 0.0, 0.0
    for price, qty in fills:
        price = sf(price)
        qty = sf(qty)
        if price <= 0 or qty <= 0:
            continue
        per_share = price + fee_usdc(1.0, price)
        affordable = max(0.0, (max_total - spent) / per_share)
        take = min(qty, affordable)
        if take <= 1e-9:
            break
        out.append((price, take))
        spent += price * take + fee_usdc(take, price)
        shares += take
        if spent >= max_total - 1e-8:
            break
    return out, shares


def stop_triggered(condition, variant_name):
    with db() as conn:
        return bool(conn.execute(
            "SELECT 1 FROM stop_events WHERE condition_id=? AND variant=?",
            (condition, variant_name),
        ).fetchone())


def position_totals(condition, variant_name):
    with db() as conn:
        buys = conn.execute(
            "SELECT * FROM paper_trades WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant_name),
        ).fetchall()
        exits = conn.execute(
            "SELECT * FROM paper_exits WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant_name),
        ).fetchall()
    bought = sum(sf(r["filled_shares"]) for r in buys)
    exited = sum(sf(r["filled_shares"]) for r in exits)
    buy_cost = sum(sf(r["total_cost"]) for r in buys)
    exit_net = sum(sf(r["net_proceeds"]) for r in exits)
    primary_asset = str(buys[0]["asset"]) if buys else None
    primary_outcome = str(buys[0]["outcome"]) if buys else None
    pyramid_trades = sum(1 for r in buys if str(r["signal_type"]).upper() == "PYRAMID")
    dca_trades = sum(1 for r in buys if str(r["signal_type"]).upper() == "DCA")
    return {
        "buys": buys,
        "exits": exits,
        "bought": bought,
        "exited": exited,
        "remaining": max(0.0, bought - exited),
        "buy_cost": buy_cost,
        "exit_net": exit_net,
        "primary_asset": primary_asset,
        "primary_outcome": primary_outcome,
        "pyramid_trades": pyramid_trades,
        "has_pyramid": pyramid_trades > 0,
        "dca_trades": dca_trades,
        "has_dca": dca_trades > 0,
    }


def breakeven_event(condition, variant_name):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM breakeven_events WHERE condition_id=? AND variant=?",
            (condition, variant_name),
        ).fetchone()


def weighted_gross_entry_avg(pos):
    bought = sum(sf(r["filled_shares"]) for r in pos["buys"])
    gross = sum(sf(r["gross_cost"]) for r in pos["buys"])
    return (gross / bought) if bought > 1e-9 else None


def fee_adjusted_profit_stop_price(pos, target_profit_usdc=None):
    """Price/share whose modeled full-position exit leaves target net PnL.

    The target is TOTAL USDC profit for this strategy position, not profit/share.
    Entry fees are already included in buy_cost. Previous exit proceeds reduce the
    remaining net amount needed. The future sell fee uses the same crypto fee model,
    so the calculated stop includes both buy-side and sell-side modeled fees.
    """
    remaining = sf(pos.get("remaining"))
    if remaining <= 1e-9:
        return None
    target = BREAKEVEN_MIN_PROFIT_USDC if target_profit_usdc is None else max(0.0, sf(target_profit_usdc))
    need_net = max(0.0, sf(pos.get("buy_cost")) + target - sf(pos.get("exit_net")))
    if need_net <= 1e-12:
        return 0.0

    def net_at(price):
        return remaining * price - fee_usdc(remaining, price)

    if net_at(1.0) + 1e-9 < need_net:
        return None
    lo, hi = 0.0, 1.0
    for _ in range(64):
        mid = (lo + hi) / 2.0
        if net_at(mid) >= need_net:
            hi = mid
        else:
            lo = mid
    return hi


# Backward-compatible helper name for old local tests/imports.
def fee_adjusted_breakeven_price(pos):
    return fee_adjusted_profit_stop_price(pos)


def arm_breakeven(condition, variant, bid, entry_avg, arm_trigger, stop_price):
    with db() as conn:
        cur = conn.execute("""
            INSERT OR IGNORE INTO breakeven_events(
                condition_id,variant,armed_ms,arm_bid,entry_avg_price,
                arm_trigger_price,stop_price
            ) VALUES(?,?,?,?,?,?,?)
        """, (
            condition, variant["name"], now_ms(), bid, entry_avg,
            arm_trigger, stop_price,
        ))
        conn.commit()
        return cur.rowcount > 0


def mark_breakeven_triggered(condition, variant, bid, stop_price):
    ts = now_ms()
    with db() as conn:
        conn.execute("""
            UPDATE breakeven_events
            SET triggered_ms=COALESCE(triggered_ms,?),
                trigger_bid=COALESCE(trigger_bid,?), stop_price=?
            WHERE condition_id=? AND variant=?
        """, (ts, bid, stop_price, condition, variant["name"]))
        conn.execute("""
            INSERT OR IGNORE INTO stop_events(
                condition_id,variant,trigger_ms,trigger_bid,stop_price
            ) VALUES(?,?,?,?,?)
        """, (condition, variant["name"], ts, bid, stop_price))
        conn.commit()
    st = get_variant_state(condition, variant)
    st["stopped_out"] = True


def update_breakeven_completion(condition, variant, filled, avg, fee, net, completed=False):
    with db() as conn:
        conn.execute("""
            UPDATE breakeven_events
            SET exit_filled_shares=COALESCE(exit_filled_shares,0)+?,
                exit_fee=COALESCE(exit_fee,0)+?,
                exit_net=COALESCE(exit_net,0)+?,
                exit_avg_price=CASE
                    WHEN COALESCE(exit_filled_shares,0)+? > 0 THEN
                        ((COALESCE(exit_avg_price,0)*COALESCE(exit_filled_shares,0)) + (?*?)) /
                        (COALESCE(exit_filled_shares,0)+?)
                    ELSE exit_avg_price END,
                completed_ms=CASE WHEN ? THEN COALESCE(completed_ms,?) ELSE completed_ms END
            WHERE condition_id=? AND variant=?
        """, (
            filled, fee, net,
            filled, avg, filled, filled,
            1 if completed else 0, now_ms(), condition, variant["name"],
        ))
        conn.commit()


async def execute_paper_exit_all(condition, variant, asset, outcome, reason, trigger_price):
    age = await ensure_book(asset)
    pos = position_totals(condition, variant["name"])
    wanted = sf(pos.get("remaining"))
    if wanted <= 1e-9:
        return {"filled": 0.0, "remaining": 0.0, "complete": True}

    fills, filled = simulate_sell(asset, wanted)
    if filled <= 1e-9:
        return {"filled": 0.0, "remaining": wanted, "complete": False}

    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    net = gross - fee
    avg = gross / filled
    name = variant["name"]
    cash = paper_cash(name)
    after = cash + net

    b = books.get(asset) or {}
    with db() as conn:
        conn.execute("""
            INSERT INTO paper_exits(
                exit_ms,condition_id,variant,asset,outcome,reason,trigger_price,
                requested_shares,filled_shares,avg_price,gross_proceeds,fee,
                net_proceeds,book_age_ms,book_received_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, name, asset, outcome, reason, trigger_price,
            wanted, filled, avg, gross, fee, net, age, si(b.get("received_ms")),
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{name}", str(after)),
        )
        conn.commit()

    remaining = max(0.0, wanted - filled)
    log.warning(
        "BE STOP SELL %-20s %-4s | %.2f/%.2fsh @ %.4f fee=%.4f | net %.4f | remaining %.4f",
        name, outcome, filled, wanted, avg, fee, net, remaining,
    )
    return {
        "filled": filled, "remaining": remaining, "complete": remaining <= 1e-8,
        "avg": avg, "fee": fee, "net": net,
    }


async def process_breakeven_stop(market, variant, elapsed):
    """Arm at +trigger move, then liquidate at the fee-adjusted profit floor.

    The default floor targets +$0.10 total modeled PnL after entry and exit fees.
    Protection stays active even when global trading is STOPPED. Once triggered,
    liquidation attempts continue on following decision cycles until the paper
    position is fully closed or the market ends.
    """
    if not BREAKEVEN_STOP_ENABLED or not variant.get("breakeven_stop_enabled", True):
        return False

    cid = market["condition_id"]
    pos = position_totals(cid, variant["name"])
    if not pos["buys"] or pos["remaining"] <= 1e-9:
        return False

    asset = pos["primary_asset"]
    if not asset:
        return False
    bid = best_bid(asset)
    if bid is None:
        return False
    outcome = pos["primary_outcome"] or ("Up" if asset == market["up_asset"] else "Down")

    entry_avg = weighted_gross_entry_avg(pos)
    stop_price = fee_adjusted_profit_stop_price(pos)
    if entry_avg is None or stop_price is None:
        return False
    configured_arm_trigger = entry_avg + BREAKEVEN_TRIGGER_MOVE
    # Normally +0.05 on a 5-share position is already enough to lock +$0.10.
    # For a rare partial ENTRY fill, wait until the modeled +profit stop itself
    # is executable so we do not arm before the requested profit actually exists.
    arm_trigger = max(configured_arm_trigger, stop_price)

    ev = breakeven_event(cid, variant["name"])
    if ev is None:
        if bid + 1e-12 < arm_trigger:
            return False
        if arm_breakeven(cid, variant, bid, entry_avg, arm_trigger, stop_price):
            log.info(
                "BE ARMED %-24s %s | %s bid=%.4f | configured-arm=%.4f | effective-arm=%.4f | profit-stop=%.4f target=$%.2f",
                variant["name"], cid[-6:], outcome, bid,
                configured_arm_trigger, arm_trigger, stop_price, BREAKEVEN_MIN_PROFIT_USDC,
            )
        return True  # never trigger on the arming tick

    # Recalculate stop from the actual total position/cost basis (important if H DCA
    # occurred before arming or if a prior stop attempt only partially filled).
    with db() as conn:
        conn.execute(
            "UPDATE breakeven_events SET stop_price=? WHERE condition_id=? AND variant=?",
            (stop_price, cid, variant["name"]),
        )
        conn.commit()

    triggered = ev["triggered_ms"] is not None or stop_triggered(cid, variant["name"])
    if not triggered:
        if bid > stop_price + 1e-12:
            return True
        mark_breakeven_triggered(cid, variant, bid, stop_price)
        log.warning(
            "BE TRIGGER %-22s %s | %s bid=%.4f <= profit-stop=%.4f | target=$%.2f | elapsed=%.1fs",
            variant["name"], cid[-6:], outcome, bid, stop_price, BREAKEVEN_MIN_PROFIT_USDC, elapsed,
        )

    # Once triggered, keep trying to flatten even if price bounces back above stop.
    result = await execute_paper_exit_all(
        cid, variant, asset, outcome, "BREAKEVEN_STOP", stop_price
    )
    if result.get("filled", 0.0) > 1e-9:
        update_breakeven_completion(
            cid, variant,
            sf(result.get("filled")), sf(result.get("avg")),
            sf(result.get("fee")), sf(result.get("net")),
            bool(result.get("complete")),
        )
    return True


async def execute_paper(condition, variant, asset, outcome, signal_type):
    age = await ensure_book(asset)

    # Once the breakeven stop has triggered, never add risk afterward.
    if stop_triggered(condition, variant["name"]):
        return False

    wanted = ENTRY_ORDER_SIZE if signal_type == "ENTRY" else DCA_ORDER_SIZE
    fills, filled = simulate_buy(asset, wanted)
    if filled <= 0:
        return False

    name = variant["name"]
    cash = paper_cash(name)
    available = max(0.0, cash - MIN_FREE_CASH)
    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    total = gross + fee

    if total > available + 1e-8:
        fills, filled = trim_fills_to_budget(fills, available)
        if filled <= 1e-8:
            log.warning("CASH BLOCK %s %s %s | cash=%.2f", name, signal_type, outcome, cash)
            return False
        gross = sum(p * q for p, q in fills)
        fee = sum(fee_usdc(q, p) for p, q in fills)
        total = gross + fee

    avg = gross / filled
    after = cash - total
    with db() as conn:
        conn.execute("""
            INSERT INTO paper_trades(
                trade_ms,condition_id,variant,asset,outcome,signal_type,
                requested_shares,filled_shares,avg_price,gross_cost,fee,
                total_cost,book_age_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, name, asset, outcome, signal_type,
            wanted, filled, avg, gross, fee, total, age,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{name}", str(after)),
        )
        conn.commit()

    st = get_variant_state(condition, variant)
    st["buys"][asset] += 1
    st["last_buy"][asset] = avg
    st["started_sides"].add(asset)
    if st["primary_asset"] is None:
        st["primary_asset"] = asset

    log.info(
        "BUY %-20s %-7s %-4s | %.2fsh @ %.4f fee=%.4f | cash %.2f -> %.2f",
        name, signal_type, outcome, filled, avg, fee, cash, after,
    )
    return True


def _first_v2_eligible_candidates(market, variant):
    cid = market["condition_id"]
    out = []
    for asset, outcome in ((market["up_asset"], "Up"), (market["down_asset"], "Down")):
        ask = best_ask(asset)
        if ask is None or not (variant["v2_price_min"] <= ask <= variant["v2_price_max"]):
            continue
        mom, ref = momentum_for(cid, asset, variant["lookback"])
        if mom is None:
            continue
        if mom < variant["v2_mom_min"] or mom > variant["v2_mom_max"]:
            continue
        out.append((mom, asset, outcome, ask, ref))
    out.sort(reverse=True, key=lambda x: x[0])
    return out


def record_first_v2_vote(market, elapsed, decision_ms):
    """Persist one market-wide FIRST V2-eligible direction vote per token/market."""
    cid = market["condition_id"]
    with db() as conn:
        exists = conn.execute(
            "SELECT 1 FROM v2_votes WHERE condition_id=? LIMIT 1", (cid,)
        ).fetchone()
    if exists:
        return

    symbol = market_symbol(market)
    variants = STRATEGIES_BY_SYMBOL.get(symbol) or []
    if not variants:
        return
    probe = variants[0]  # all F/G/H/J share identical V2 eligibility.
    candidates = _first_v2_eligible_candidates(market, probe)
    if not candidates:
        return

    mom, asset, outcome, ask, ref = candidates[0]
    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO v2_votes(
                condition_id,symbol,decision_ms,asset,outcome,ask,
                reference_ask,momentum,elapsed_sec
            ) VALUES(?,?,?,?,?,?,?,?,?)
        """, (
            cid, symbol, int(decision_ms), asset, outcome, ask,
            ref, mom, elapsed,
        ))
        conn.commit()

    log.info(
        "V2 VOTE %-4s %s | %s %.3f mom=%+.3f",
        symbol, cid[-6:], outcome, ask, mom,
    )


def first_v2_vote(condition_id):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM v2_votes WHERE condition_id=?", (condition_id,)
        ).fetchone()


def consensus_confirmations(target_symbol, outcome, at_ms, window_sec):
    """Latest FIRST V2-eligible vote from each DISTINCT OTHER token in the window."""
    cutoff = int(at_ms - float(window_sec) * 1000.0)
    with db() as conn:
        rows = conn.execute("""
            SELECT symbol,decision_ms,condition_id,ask,momentum,outcome
            FROM v2_votes
            WHERE outcome=?
              AND decision_ms>=?
              AND decision_ms<=?
              AND symbol<>?
            ORDER BY decision_ms DESC
        """, (outcome, cutoff, at_ms, target_symbol)).fetchall()

    latest = {}
    for r in rows:
        symbol = str(r["symbol"] or "").upper()
        if symbol not in SYMBOLS or symbol == target_symbol:
            continue
        if symbol not in latest:
            latest[symbol] = int(r["decision_ms"])

    ordered = sorted(latest.items(), key=lambda kv: kv[1], reverse=True)
    symbols = [s for s, _ in ordered]
    ages = [max(0, int(at_ms - ms)) for _, ms in ordered]
    return symbols, ages


def store_consensus_event(condition, variant, symbol, outcome, ask, mom, at_ms,
                          confirm_symbols, confirm_ages, passed, reason):
    with db() as conn:
        conn.execute("""
            INSERT INTO consensus_events(
                condition_id,variant,decision_ms,target_symbol,target_outcome,
                target_ask,target_momentum,window_sec,required_count,confirm_count,
                confirm_symbols_json,confirm_ages_ms_json,passed,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (
            condition, variant["name"], at_ms, symbol, outcome, ask, mom,
            float(variant["consensus_window_sec"]),
            int(variant["consensus_min_other_tokens"]),
            len(confirm_symbols), jd(confirm_symbols), jd(confirm_ages),
            1 if passed else 0, reason,
        ))
        conn.commit()


async def evaluate_consensus_variant(market, variant, elapsed):
    """F/G/H/J: SAFE67 target gated by recent FIRST-V2 votes from other tokens."""
    cid = market["condition_id"]
    symbol = market_symbol(market)
    st = get_variant_state(cid, variant)

    if st.get("stopped_out"):
        return

    if not st["gate_decided"] and not st["started_sides"]:
        vote = first_v2_vote(cid)
        if vote is None:
            # Defensive fallback for direct tests/calls outside strategy_loop.
            record_first_v2_vote(market, elapsed, now_ms())
            vote = first_v2_vote(cid)
        if vote is None:
            return

        mom = sf(vote["momentum"])
        asset = str(vote["asset"])
        outcome = str(vote["outcome"])
        ask = sf(vote["ask"])
        ref = sf(vote["reference_ask"])
        at_ms = si(vote["decision_ms"])

        price_ok = variant["safe_entry_price_min"] <= ask <= variant["safe_entry_price_max"]
        mom_ok = variant["safe_entry_mom_min"] <= mom <= variant["safe_entry_mom_max"]
        safe_ok = bool(price_ok and mom_ok)

        confirm_symbols, confirm_ages = [], []
        consensus_ok = False
        if safe_ok:
            confirm_symbols, confirm_ages = consensus_confirmations(
                symbol, outcome, at_ms, variant["consensus_window_sec"]
            )
            consensus_ok = len(confirm_symbols) >= int(variant["consensus_min_other_tokens"])

        passed = bool(safe_ok and consensus_ok)
        st["gate_decided"] = True
        st["gate_passed"] = passed
        st["gate_asset"] = asset if passed else None

        if ask < variant["safe_entry_price_min"]:
            reason = "SAFE_PRICE_LOW"
        elif ask > variant["safe_entry_price_max"]:
            reason = "SAFE_PRICE_HIGH"
        elif mom < variant["safe_entry_mom_min"]:
            reason = "SAFE_MOMENTUM_LOW"
        elif mom > variant["safe_entry_mom_max"]:
            reason = "SAFE_MOMENTUM_HIGH"
        elif not consensus_ok:
            reason = "V2_CONSENSUS_INSUFFICIENT"
        else:
            reason = "V2_CONSENSUS_OK"

        store_gate_decision(
            cid, variant, asset, outcome, ask, ref, mom, elapsed, passed, reason
        )
        store_consensus_event(
            cid, variant, symbol, outcome, ask, mom, at_ms,
            confirm_symbols, confirm_ages, passed, reason
        )

        log.info(
            "V2 CONS %-24s %s | %s %.3f mom=%+.3f | need=%d votes=%d [%s] | %s",
            variant["name"], cid[-6:], outcome, ask, mom,
            int(variant["consensus_min_other_tokens"]), len(confirm_symbols),
            ",".join(confirm_symbols) if confirm_symbols else "-",
            "PASS" if passed else f"SKIP {reason}",
        )
        if not passed:
            return

    if st["gate_decided"] and not st["gate_passed"]:
        return

    # ENTRY execution still uses the current held-side book/momentum, exactly
    # as the prior SAFE67 engine did after its gate passed.
    if not st["started_sides"]:
        asset = st.get("gate_asset")
        if not asset:
            return
        outcome = "Up" if asset == market["up_asset"] else "Down"
        ask = best_ask(asset)
        if ask is None:
            return
        mom, ref = momentum_for(cid, asset, variant["lookback"])
        if mom is None:
            return
        if not (variant["safe_entry_price_min"] <= ask <= variant["safe_entry_price_max"]):
            return
        if not (variant["safe_entry_mom_min"] <= mom <= variant["safe_entry_mom_max"]):
            return
        store_signal(cid, variant, asset, outcome, ask, ref, mom, "ENTRY", elapsed)
        await execute_paper(cid, variant, asset, outcome, "ENTRY")
        return

    # F/G/J stop after the 5-share ENTRY.
    if not variant.get("dca_enabled"):
        return

    # H only: same safer reversal DCA rules as C.
    asset = st.get("primary_asset")
    if not asset or st["buys"][asset] >= variant["max_buys_side"]:
        return
    if elapsed > variant["dca_deadline_sec"]:
        return

    ask = best_ask(asset)
    if ask is None or ask < MIN_PRICE or ask > MAX_PRICE:
        return
    outcome = "Up" if asset == market["up_asset"] else "Down"

    # Arm on weakness, but never buy on the arm tick.
    if not st.get("dca_armed"):
        if ask <= variant["dca_arm_price"] + 1e-12:
            arm_dca(cid, variant, ask, elapsed)
            log.info(
                "DCA ARMED %-21s %s | %s ask=%.3f <= %.3f | elapsed=%.1fs",
                variant["name"], cid[-6:], outcome, ask,
                variant["dca_arm_price"], elapsed,
            )
        return

    # Later rebound must satisfy BOTH the safer price range and momentum band.
    mom, ref = momentum_for(cid, asset, variant["lookback"])
    if mom is None:
        return
    if mom < variant["dca_rebound_mom"]:
        return
    mom_max = variant.get("dca_rebound_mom_max")
    if mom_max is not None and mom > float(mom_max) + 1e-12:
        return
    if ask < float(variant["dca_min_buy_price"]) - 1e-12:
        return
    if ask > float(variant["dca_max_buy_price"]) + 1e-12:
        return

    store_signal(cid, variant, asset, outcome, ask, ref, mom, "DCA", elapsed)
    filled = await execute_paper(cid, variant, asset, outcome, "DCA")
    if filled:
        mark_dca_filled(cid, variant, ask, mom, elapsed)
        log.info(
            "DCA FILLED %-20s %s | %s ask=%.3f mom=%+.3f | total buys=%d",
            variant["name"], cid[-6:], outcome, ask, mom, st["buys"][asset],
        )


# Breakeven stop is processed before strategy ENTRY/DCA in strategy_loop.


def record_position_trajectory(market, variant, elapsed):
    cid = market["condition_id"]
    with db() as conn:
        if conn.execute(
            "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
            (cid, variant["name"]),
        ).fetchone():
            return False

    pos = position_totals(cid, variant["name"])
    if not pos["buys"]:
        return False

    primary_asset = pos["primary_asset"]
    primary_outcome = pos["primary_outcome"]
    opposite_asset = str(market["down_asset"] if primary_asset == str(market["up_asset"]) else market["up_asset"])
    remaining = pos["remaining"]

    p_bid = best_bid(primary_asset)
    p_ask = best_ask(primary_asset)
    o_bid = best_bid(opposite_asset)
    o_ask = best_ask(opposite_asset)

    mark_fills, mark_filled = simulate_sell(primary_asset, remaining) if remaining > 1e-9 else ([], 0.0)
    mark_gross = sum(sf(px) * sf(q) for px, q in mark_fills)
    mark_fee = sum(fee_usdc(sf(q), sf(px)) for px, q in mark_fills)
    mark_net = mark_gross - mark_fee
    mark_avg = mark_gross / mark_filled if mark_filled > 1e-9 else None

    # Total PnL if all remaining shares could be liquidated now.
    unrealized = None
    if remaining <= 1e-9:
        unrealized = pos["exit_net"] - pos["buy_cost"]
    elif mark_filled >= remaining - 1e-8:
        unrealized = pos["exit_net"] + mark_net - pos["buy_cost"]

    with db() as conn:
        prev = conn.execute("""
            SELECT MAX(unrealized_total_pnl) mfe, MIN(unrealized_total_pnl) mae
            FROM position_trajectory
            WHERE condition_id=? AND variant=? AND unrealized_total_pnl IS NOT NULL
        """, (cid, variant["name"])).fetchone()
        prev_mfe = sf(prev["mfe"]) if prev and prev["mfe"] is not None else None
        prev_mae = sf(prev["mae"]) if prev and prev["mae"] is not None else None
        mfe = prev_mfe if unrealized is None else (unrealized if prev_mfe is None else max(prev_mfe, unrealized))
        mae = prev_mae if unrealized is None else (unrealized if prev_mae is None else min(prev_mae, unrealized))

        conn.execute("""
            INSERT INTO position_trajectory(
                sample_ms,condition_id,variant,elapsed_sec,primary_asset,primary_outcome,
                opposite_asset,bought_shares,exited_shares,remaining_shares,gross_entry_cost,
                entry_fees,total_buy_cost,exit_net_so_far,primary_best_bid,primary_best_ask,
                opposite_best_bid,opposite_best_ask,mark_filled_shares,mark_avg_price,mark_fee,
                mark_net_proceeds,unrealized_total_pnl,mfe_pnl,mae_pnl,stop_triggered
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), cid, variant["name"], elapsed, primary_asset, primary_outcome,
            opposite_asset, pos["bought"], pos["exited"], remaining,
            sum(sf(r["gross_cost"]) for r in pos["buys"]),
            sum(sf(r["fee"]) for r in pos["buys"]), pos["buy_cost"], pos["exit_net"],
            p_bid, p_ask, o_bid, o_ask, mark_filled, mark_avg, mark_fee, mark_net,
            unrealized, mfe, mae, 1 if stop_triggered(cid, variant["name"]) else 0,
        ))
        conn.commit()
    return True


async def strategy_loop():
    while True:
        started = time.monotonic()
        n = time.time()
        try:
            active = []
            trade_ready = []

            # One WebSocket-book sample/history tick for every active market.
            # No pre-decision REST refresh.
            for cid, market in list(markets.items()):
                elapsed = n - market["start_ts"]
                if not (-30 <= elapsed <= 310):
                    continue

                for asset in (market["up_asset"], market["down_asset"]):
                    ask = best_ask(asset)
                    if ask is not None:
                        price_history[cid][asset].append((now_ms(), ask))

                variants = strategies_for_market(market)
                if 0 <= elapsed <= 305:
                    active.append((market, elapsed, variants))
                    for variant in variants:
                        record_position_trajectory(market, variant, elapsed)

                if elapsed < 0 or elapsed > TRADE_WINDOW_SECONDS or not trading_enabled():
                    continue
                if best_ask(market["up_asset"]) is None or best_ask(market["down_asset"]) is None:
                    continue
                trade_ready.append((market, elapsed, variants))

            # Phase 0: protective BE exits. These remain active even if START is OFF.
            # Running them before DCA prevents H from adding risk after the BE stop fires.
            for market, elapsed, variants in active:
                for variant in variants:
                    await process_breakeven_stop(market, variant, elapsed)

            # Phase 1: capture the FIRST V2-eligible vote for every token using
            # one common decision-cycle timestamp.
            cycle_ms = now_ms()
            for market, elapsed, _variants in trade_ready:
                record_first_v2_vote(market, elapsed, cycle_ms)

            # Phase 2: F/G/H/J read exactly the same V2-vote tape.
            for market, elapsed, variants in trade_ready:
                for variant in variants:
                    await evaluate_consensus_variant(market, variant, elapsed)

        except Exception:
            log.exception("Strategy loop failed")

        spent = time.monotonic() - started
        await asyncio.sleep(max(0.05, DECISION_INTERVAL - spent))


# ============================================================
# RESOLUTION — EACH PAPER ACCOUNT INDEPENDENT
# ============================================================

async def settle_from_resolution(ev):
    cid = str(ev.get("market") or ev.get("condition_id") or "")
    winning_asset = str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    winning_outcome = str(ev.get("winning_outcome") or "")
    if cid and winning_asset:
        await settle_market(cid, winning_asset, winning_outcome)


async def settle_market(cid, winning_asset, winning_outcome):
    async with settle_lock:
        market = markets.get(cid)
        if not market:
            with db() as conn:
                row = conn.execute("SELECT * FROM discovered_markets WHERE condition_id=?", (cid,)).fetchone()
                if not row:
                    return
                market = dict(row)

        symbol = market_symbol(market)
        pair = STRATEGIES_BY_SYMBOL.get(symbol, [])
        messages = []
        with db() as conn:
            for variant in pair:
                name = variant["name"]
                if conn.execute(
                    "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchone():
                    continue

                buys = conn.execute(
                    "SELECT * FROM paper_trades WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchall()
                exits = conn.execute(
                    "SELECT * FROM paper_exits WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchall()

                buy_cost = sum(sf(r["total_cost"]) for r in buys)
                exit_proceeds = sum(sf(r["net_proceeds"]) for r in exits)
                up_bought = sum(sf(r["filled_shares"]) for r in buys if str(r["asset"]) == str(market["up_asset"]))
                down_bought = sum(sf(r["filled_shares"]) for r in buys if str(r["asset"]) == str(market["down_asset"]))
                up_exited = sum(sf(r["filled_shares"]) for r in exits if str(r["asset"]) == str(market["up_asset"]))
                down_exited = sum(sf(r["filled_shares"]) for r in exits if str(r["asset"]) == str(market["down_asset"]))

                winning_bought = sum(sf(r["filled_shares"]) for r in buys if str(r["asset"]) == str(winning_asset))
                winning_exited = sum(sf(r["filled_shares"]) for r in exits if str(r["asset"]) == str(winning_asset))
                winning_remaining = max(0.0, winning_bought - winning_exited)
                payout = winning_remaining
                pnl = exit_proceeds + payout - buy_cost
                stopped = 1 if conn.execute(
                    "SELECT 1 FROM stop_events WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchone() else 0

                conn.execute("""
                    INSERT INTO market_results(
                        condition_id,variant,winning_asset,winning_outcome,buy_cost,
                        exit_proceeds,payout,pnl,buy_trades,exit_trades,up_bought,
                        down_bought,up_exited,down_exited,stopped_out,settled_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    cid, name, winning_asset, winning_outcome, buy_cost,
                    exit_proceeds, payout, pnl, len(buys), len(exits), up_bought,
                    down_bought, up_exited, down_exited, stopped, now_ms(),
                ))

                cash_row = conn.execute(
                    "SELECT value FROM state WHERE key=?", (f"paper_cash:{name}",)
                ).fetchone()
                cash_before = sf(cash_row["value"] if cash_row else PAPER_START_BALANCE, PAPER_START_BALANCE)
                cash_after = cash_before + payout
                conn.execute(
                    "INSERT INTO state(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"paper_cash:{name}", str(cash_after)),
                )

                if buys:
                    messages.append(
                        f"{variant['short']}: {pnl:+.2f} | cash ${cash_after:.2f}" + (" | STOP" if stopped else "")
                    )

            conn.execute("""
                UPDATE discovered_markets
                SET resolved=1,winning_asset=?,winning_outcome=?
                WHERE condition_id=?
            """, (winning_asset, winning_outcome, cid))
            conn.commit()

        if messages:
            log.info("RESOLVED %s %s | %s", symbol, cid[-6:], " | ".join(messages))
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"✅ {symbol} MARKET SETTLED | {winning_outcome or winning_asset[-8:]}\n" + "\n".join(messages)
                )


def resolve_winner_from_market(market_row):
    if not isinstance(market_row, dict):
        return None, None
    outcomes = [str(x) for x in parse_jsonish(market_row.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(market_row.get("clobTokenIds"))]
    prices_raw = parse_jsonish(market_row.get("outcomePrices"))

    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices_raw) >= 2:
        prices = [sf(x, -1) for x in prices_raw]
        best_idx = max(range(len(prices)), key=lambda i: prices[i])
        best = prices[best_idx]
        others = [prices[i] for i in range(len(prices)) if i != best_idx]
        second = max(others) if others else -1
        closed = bool(market_row.get("closed", False))
        resolved_flag = bool(
            market_row.get("resolved", False)
            or market_row.get("umaResolutionStatus") == "resolved"
        )
        if best >= 0.999 and second <= 0.001 and (closed or resolved_flag or best >= 0.9999):
            return tokens[best_idx], outcomes[best_idx]

    token_objs = market_row.get("tokens")
    if isinstance(token_objs, list):
        for tok in token_objs:
            if isinstance(tok, dict) and bool(tok.get("winner", False)):
                asset = str(tok.get("token_id") or tok.get("tokenId") or tok.get("id") or "")
                outcome = str(tok.get("outcome") or tok.get("name") or "")
                if asset:
                    return asset, outcome
    return None, None


async def fetch_resolved_market_by_slug(slug, condition_id):
    event = await fetch_event_by_slug(slug)
    if not isinstance(event, dict) or not isinstance(event.get("markets"), list):
        return None
    embedded = event["markets"]
    for m in embedded:
        if isinstance(m, dict):
            cid = str(m.get("conditionId") or m.get("condition_id") or "")
            if cid == str(condition_id):
                return m
    if len(embedded) == 1 and isinstance(embedded[0], dict):
        return embedded[0]
    return None


async def resolution_fallback_loop():
    while True:
        try:
            cutoff = now_ts() - 10
            with db() as conn:
                rows = conn.execute("""
                    SELECT condition_id,slug,question,end_ts
                    FROM discovered_markets
                    WHERE resolved=0 AND end_ts<?
                    ORDER BY end_ts LIMIT 50
                """, (cutoff,)).fetchall()

            for row in rows:
                cid = str(row["condition_id"])
                slug = str(row["slug"] or "")
                if not slug:
                    continue
                m = await fetch_resolved_market_by_slug(slug, cid)
                if not m:
                    continue
                winning_asset, winning_outcome = resolve_winner_from_market(m)
                if winning_asset:
                    log.info("RESOLUTION FALLBACK %s | winner=%s", slug, winning_outcome or winning_asset[-8:])
                    await settle_market(cid, winning_asset, winning_outcome)
        except Exception:
            log.exception("Resolution fallback failed")
        await asyncio.sleep(10)




# ============================================================
# PAPER ACCOUNTS / TELEGRAM CONTROL
# ============================================================

def open_cost_basis(strategy_name):
    with db() as conn:
        rows = conn.execute("""
            SELECT t.condition_id,
                   SUM(t.filled_shares) bought,
                   SUM(t.total_cost) buy_cost
            FROM paper_trades t
            LEFT JOIN market_results r
              ON r.condition_id=t.condition_id AND r.variant=t.variant
            WHERE t.variant=? AND r.condition_id IS NULL
            GROUP BY t.condition_id
        """, (strategy_name,)).fetchall()
        total = 0.0
        for r in rows:
            exited = sf(conn.execute(
                "SELECT COALESCE(SUM(filled_shares),0) x FROM paper_exits WHERE condition_id=? AND variant=?",
                (r["condition_id"], strategy_name),
            ).fetchone()["x"])
            bought = sf(r["bought"])
            remaining = max(0.0, bought - exited)
            if bought > 1e-9:
                total += sf(r["buy_cost"]) * remaining / bought
        return total


def account_stats(strategy_name):
    cash = paper_cash(strategy_name)
    initial = paper_initial(strategy_name)
    with db() as conn:
        realized = sf(conn.execute(
            "SELECT COALESCE(SUM(pnl),0) p FROM market_results WHERE variant=?", (strategy_name,)
        ).fetchone()["p"])
        traded = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0", (strategy_name,)
        ).fetchone()["c"])
        wins = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0 AND pnl>0", (strategy_name,)
        ).fetchone()["c"])
        losses = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0 AND pnl<0", (strategy_name,)
        ).fetchone()["c"])
        buys = si(conn.execute(
            "SELECT COUNT(*) c FROM paper_trades WHERE variant=?", (strategy_name,)
        ).fetchone()["c"])
        exits = si(conn.execute(
            "SELECT COUNT(*) c FROM paper_exits WHERE variant=?", (strategy_name,)
        ).fetchone()["c"])
        buy_fees = sf(conn.execute(
            "SELECT COALESCE(SUM(fee),0) f FROM paper_trades WHERE variant=?", (strategy_name,)
        ).fetchone()["f"])
        exit_fees = sf(conn.execute(
            "SELECT COALESCE(SUM(fee),0) f FROM paper_exits WHERE variant=?", (strategy_name,)
        ).fetchone()["f"])
        avg_win = sf(conn.execute(
            "SELECT COALESCE(AVG(pnl),0) x FROM market_results WHERE variant=? AND pnl>0", (strategy_name,)
        ).fetchone()["x"])
        avg_loss = sf(conn.execute(
            "SELECT COALESCE(AVG(pnl),0) x FROM market_results WHERE variant=? AND pnl<0", (strategy_name,)
        ).fetchone()["x"])
        worst = sf(conn.execute(
            "SELECT COALESCE(MIN(pnl),0) x FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0", (strategy_name,)
        ).fetchone()["x"])
        gate_pass = si(conn.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE variant=? AND passed=1", (strategy_name,)
        ).fetchone()["c"])
        gate_skip = si(conn.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE variant=? AND passed=0", (strategy_name,)
        ).fetchone()["c"])
        stops = si(conn.execute(
            "SELECT COUNT(*) c FROM stop_events WHERE variant=?", (strategy_name,)
        ).fetchone()["c"])
        be_armed = si(conn.execute(
            "SELECT COUNT(*) c FROM breakeven_events WHERE variant=?", (strategy_name,)
        ).fetchone()["c"])
        dca_armed = si(conn.execute(
            "SELECT COUNT(*) c FROM dca_events WHERE variant=?", (strategy_name,)
        ).fetchone()["c"])
        dca_filled = si(conn.execute(
            "SELECT COUNT(*) c FROM dca_events WHERE variant=? AND filled_ms IS NOT NULL", (strategy_name,)
        ).fetchone()["c"])
        consensus_checked = si(conn.execute(
            "SELECT COUNT(*) c FROM consensus_events WHERE variant=?", (strategy_name,)
        ).fetchone()["c"])
        consensus_passed = si(conn.execute(
            "SELECT COUNT(*) c FROM consensus_events WHERE variant=? AND passed=1", (strategy_name,)
        ).fetchone()["c"])

    oc = open_cost_basis(strategy_name)
    return {
        "initial": initial,
        "cash": cash,
        "open_cost": oc,
        "equity_cost": cash + oc,
        "realized": realized,
        "traded_markets": traded,
        "wins": wins,
        "losses": losses,
        "buy_trades": buys,
        "exit_trades": exits,
        "fees": buy_fees + exit_fees,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "worst": worst,
        "gate_pass": gate_pass,
        "gate_skip": gate_skip,
        "stops": stops,
        "be_armed": be_armed,
        "dca_armed": dca_armed,
        "dca_filled": dca_filled,
        "consensus_checked": consensus_checked,
        "consensus_passed": consensus_passed,
    }


def keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "💰 BALANCE"}, {"text": "📊 STATISTICS"}],
            [{"text": "📈 POSITIONS"}, {"text": "📜 TRADES"}],
            [{"text": "🟢 PAPER"}, {"text": "🔴 LIVE"}],
            [{"text": "🚨 EMERGENCY STOP"}],
        ],
        "resize_keyboard": True,
    }


async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or session is None:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": str(text)[:4096], "reply_markup": keyboard()},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                log.warning("Telegram message failed: %s", await r.text())
                return False
        return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def format_balance(strategy, s):
    return (
        f"{strategy['code']} — {strategy['short']}\n"
        f"Cash: ${s['cash']:.2f} | Realized: ${s['realized']:+.2f}\n"
        f"Open cost: ${s['open_cost']:.2f} | Equity: ${s['equity_cost']:.2f}"
    )


async def send_balance():
    for symbol in SYMBOLS:
        variants = STRATEGIES_BY_SYMBOL[symbol]
        await tg_send(
            f"💰 {symbol} F/G/H/J\n\n"
            + "\n\n".join(format_balance(v, account_stats(v["name"])) for v in variants)
            + f"\n\nGlobal trading: {'ON' if trading_enabled() else 'OFF'}"
        )


def format_stats(strategy, s):
    d = s["wins"] + s["losses"]
    wr = s["wins"] / d * 100.0 if d else 0.0
    extras = [
        f"V2 consensus pass/checked: {s['consensus_passed']}/{s['consensus_checked']}",
        f"BE arm +{BREAKEVEN_TRIGGER_MOVE:.2f} / profit ${BREAKEVEN_MIN_PROFIT_USDC:.2f} armed/stopped: {s['be_armed']}/{s['stops']}",
    ]
    if strategy.get("dca_enabled"):
        extras.append(f"DCA armed/filled: {s['dca_armed']}/{s['dca_filled']}")
    return (
        f"{strategy['code']} — {strategy['short']}\n"
        f"Markets: {s['traded_markets']} | W/L {s['wins']}/{s['losses']} ({wr:.1f}%)\n"
        f"Gate pass/skip: {s['gate_pass']}/{s['gate_skip']} | Buys: {s['buy_trades']}\n"
        f"Fees: ${s['fees']:.2f} | Avg W/L: ${s['avg_win']:+.2f}/${s['avg_loss']:+.2f}\n"
        f"Worst: ${s['worst']:+.2f} | PnL: ${s['realized']:+.2f}\n"
        + "\n".join(extras)
    )


async def send_statistics():
    for symbol in SYMBOLS:
        variants = STRATEGIES_BY_SYMBOL[symbol]
        await tg_send(
            f"📊 {symbol} V2 CONSENSUS F/G/H/J\n\n"
            + "\n\n".join(format_stats(v, account_stats(v["name"])) for v in variants)
        )


async def send_positions():
    any_open = False
    for symbol in SYMBOLS:
        lines = []
        for variant in STRATEGIES_BY_SYMBOL[symbol]:
            name = variant["name"]
            with db() as conn:
                markets_open = conn.execute("""
                    SELECT DISTINCT t.condition_id,t.outcome,MAX(t.trade_ms) last_ms
                    FROM paper_trades t
                    LEFT JOIN market_results r
                      ON r.condition_id=t.condition_id AND r.variant=t.variant
                    WHERE t.variant=? AND r.condition_id IS NULL
                    GROUP BY t.condition_id,t.outcome ORDER BY last_ms DESC LIMIT 20
                """, (name,)).fetchall()
            for r in markets_open:
                pos = position_totals(r["condition_id"], name)
                if pos["remaining"] <= 1e-8:
                    continue
                any_open = True
                st = get_variant_state(r["condition_id"], variant)
                extra = []
                if variant.get("dca_enabled") and st.get("dca_armed") and not pos.get("has_dca"):
                    extra.append("DCA ARMED")
                entry_avg = weighted_gross_entry_avg(pos)
                be_stop = fee_adjusted_profit_stop_price(pos)
                be = breakeven_event(r["condition_id"], name)
                if be is not None:
                    if be["triggered_ms"] is not None:
                        extra.append(f"PROFIT STOP TRIGGERED ~{be_stop:.3f}" if be_stop is not None else "PROFIT STOP TRIGGERED")
                    else:
                        extra.append(f"BE ARMED +${BREAKEVEN_MIN_PROFIT_USDC:.2f} stop~{be_stop:.3f}" if be_stop is not None else "BE ARMED")
                elif entry_avg is not None:
                    configured_arm = entry_avg + BREAKEVEN_TRIGGER_MOVE
                    effective_arm = max(configured_arm, be_stop) if be_stop is not None else configured_arm
                    extra.append(f"BE arm@{effective_arm:.3f}")
                suffix = (" | " + " | ".join(extra)) if extra else ""
                lines.append(
                    f"{variant['code']} {r['outcome']} {pos['remaining']:.2f}sh | "
                    f"cost ${pos['buy_cost']:.2f}{suffix}"
                )
        if lines:
            await tg_send(f"📈 {symbol} OPEN POSITIONS\n" + "\n".join(lines))
    if not any_open:
        await tg_send("📈 OPEN POSITIONS\nNone")


async def send_trades():
    for symbol in SYMBOLS:
        lines = []
        for variant in STRATEGIES_BY_SYMBOL[symbol]:
            actions = []
            with db() as conn:
                buys = conn.execute("""
                    SELECT trade_ms AS ms,outcome,signal_type AS action,filled_shares,avg_price
                    FROM paper_trades WHERE variant=? ORDER BY trade_ms DESC LIMIT 6
                """, (variant["name"],)).fetchall()
                exits = conn.execute("""
                    SELECT exit_ms AS ms,outcome,reason AS action,filled_shares,avg_price
                    FROM paper_exits WHERE variant=? ORDER BY exit_ms DESC LIMIT 6
                """, (variant["name"],)).fetchall()
            for r in buys:
                actions.append((si(r["ms"]), str(r["outcome"]), str(r["action"]), sf(r["filled_shares"]), sf(r["avg_price"])))
            for r in exits:
                actions.append((si(r["ms"]), str(r["outcome"]), str(r["action"]), sf(r["filled_shares"]), sf(r["avg_price"])))
            actions.sort(key=lambda x: x[0], reverse=True)
            for ms, outcome, action, shares, avg in actions[:6]:
                dt = datetime.fromtimestamp(ms/1000.0, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
                lines.append(
                    f"{variant['code']} {dt} {outcome} {action} {shares:.2f}sh @ {avg:.3f}"
                )
        if lines:
            await tg_send(f"📜 {symbol} LAST ACTIONS\n" + "\n".join(lines[:24]))


async def handle_tg(text):
    cmd = str(text or "").strip().upper()
    if cmd in {"/START", "▶️ START", "START"}:
        state_set("trading_enabled", "1")
        await tg_send(
            "▶️ MULTI7 V2 CONSENSUS F/G/H/J STARTED\n"
            f"Assets: {', '.join(SYMBOLS)}\n"
            f"F: ENTRY {C_SAFE_ENTRY_PRICE_MIN:.2f}–{C_SAFE_ENTRY_PRICE_MAX:.2f} + "
            f">={F_CONSENSUS_MIN_OTHER_TOKENS} other V2 vote/{CONSENSUS_WINDOW_SEC:g}s\n"
            f"G: ENTRY {C_SAFE_ENTRY_PRICE_MIN:.2f}–{C_SAFE_ENTRY_PRICE_MAX:.2f} + "
            f">={G_CONSENSUS_MIN_OTHER_TOKENS} other V2 votes/{CONSENSUS_WINDOW_SEC:g}s\n"
            f"H: G + SAFE DCA; arm <= {DCA_ARM_PRICE:.2f}; buy "
            f"{C_DCA_MIN_BUY_PRICE:.2f}–{C_DCA_MAX_BUY_PRICE:.2f}, "
            f"mom +{C_DCA_REBOUND_MOM_MIN:.2f}…+{C_DCA_REBOUND_MOM_MAX:.2f}\n"
            f"J: ENTRY {SAFE_ENTRY_PRICE_MIN:.2f}–{SAFE_ENTRY_PRICE_MAX:.2f} + "
            f">={J_CONSENSUS_MIN_OTHER_TOKENS} other V2 votes/{CONSENSUS_WINDOW_SEC:g}s\n"
            f"All ENTRY momentum 0.05–0.10 | 5sh | BE arm +{BREAKEVEN_TRIGGER_MOVE:.2f} -> protect +${BREAKEVEN_MIN_PROFIT_USDC:.2f} | PAPER only"
        )
    elif cmd in {"⏹ STOP", "STOP", "/STOP", "🚨 EMERGENCY STOP", "EMERGENCY STOP"}:
        state_set("trading_enabled", "0")
        await tg_send(f"⏹ New ENTRY/DCA stopped on all tokens. Profit protection (+${BREAKEVEN_MIN_PROFIT_USDC:.2f}) remains active for open positions.")
    elif cmd in {"💰 BALANCE", "BALANCE", "/BALANCE"}:
        await send_balance()
    elif cmd in {"📊 STATISTICS", "STATISTICS", "/STATS"}:
        await send_statistics()
    elif cmd in {"📈 POSITIONS", "POSITIONS"}:
        await send_positions()
    elif cmd in {"📜 TRADES", "TRADES"}:
        await send_trades()
    elif cmd in {"🟢 PAPER", "PAPER"}:
        await tg_send("🟢 PAPER mode. No real Polymarket orders are sent.")
    elif cmd in {"🔴 LIVE", "LIVE"}:
        await tg_send("🔒 LIVE is disabled. This is a clean F/G/H/J PAPER experiment.")
    else:
        await tg_send(
            "MULTI7 V2 CONSENSUS F/G/H/J\n"
            "F = tight 0.67–0.70 + >=1 other first-V2 vote\n"
            "G = tight 0.67–0.70 + >=2 other first-V2 votes\n"
            "H = G + safer reversal DCA 5sh\n"
            "J = wide 0.67–0.75 + >=2 other first-V2 votes\n"
            f"Consensus window: previous {CONSENSUS_WINDOW_SEC:g}s | BE arms after +{BREAKEVEN_TRIGGER_MOVE:.2f}; protected profit +${BREAKEVEN_MIN_PROFIT_USDC:.2f}."
        )


async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return
    offset = 0
    await tg_send(
        f"🤖 {VERSION} online\n"
        f"Assets: {', '.join(SYMBOLS)}\n"
        f"Accounts: {len(STRATEGIES)} × ${PAPER_START_BALANCE:.0f}\n"
        f"Trading: {'ON' if trading_enabled() else 'OFF'}\n"
        f"BE/profit stop: {'ON' if BREAKEVEN_STOP_ENABLED else 'OFF'} | arm +{BREAKEVEN_TRIGGER_MOVE:.2f} | target +${BREAKEVEN_MIN_PROFIT_USDC:.2f} | hourly reports OFF."
    )
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                data = await r.json()
            for update in data.get("result", []):
                offset = max(offset, si(update.get("update_id")) + 1)
                msg = update.get("message") or {}
                if str((msg.get("chat") or {}).get("id", "")) != str(TELEGRAM_CHAT_ID):
                    continue
                if msg.get("text"):
                    await handle_tg(msg["text"])
        except Exception as e:
            log.warning("Telegram polling: %s", e)
            await asyncio.sleep(2)



# ============================================================
# HEALTH / MAIN
# ============================================================

async def health(request):
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "paper_only": True,
        "trading_enabled": trading_enabled(),
        "symbols": SYMBOLS,
        "accounts": [
            {
                "symbol": v["symbol"],
                "code": v["code"],
                "name": v["name"],
                "short": v["short"],
                "cash": paper_cash(v["name"]),
                "dca_enabled": bool(v.get("dca_enabled")),
                "consensus_enabled": True,
            }
            for v in STRATEGIES
        ],
        "rules": {
            "first_v2_vote": {
                "price": [V2_ELIGIBLE_PRICE_MIN, V2_ELIGIBLE_PRICE_MAX],
                "momentum": [V2_ELIGIBLE_MOM_MIN, V2_ELIGIBLE_MOM_MAX],
                "source": "FIRST V2-eligible signal per token/market",
            },
            "target_entry_momentum": [SAFE_ENTRY_MOM_MIN, SAFE_ENTRY_MOM_MAX],
            "F": {
                "entry": [C_SAFE_ENTRY_PRICE_MIN, C_SAFE_ENTRY_PRICE_MAX],
                "other_v2_votes_min": F_CONSENSUS_MIN_OTHER_TOKENS,
                "window_sec": CONSENSUS_WINDOW_SEC,
                "dca": False,
            },
            "G": {
                "entry": [C_SAFE_ENTRY_PRICE_MIN, C_SAFE_ENTRY_PRICE_MAX],
                "other_v2_votes_min": G_CONSENSUS_MIN_OTHER_TOKENS,
                "window_sec": CONSENSUS_WINDOW_SEC,
                "dca": False,
            },
            "H": {
                "entry": [C_SAFE_ENTRY_PRICE_MIN, C_SAFE_ENTRY_PRICE_MAX],
                "other_v2_votes_min": H_CONSENSUS_MIN_OTHER_TOKENS,
                "window_sec": CONSENSUS_WINDOW_SEC,
                "dca": {
                    "arm_max": DCA_ARM_PRICE,
                    "buy_min": C_DCA_MIN_BUY_PRICE,
                    "buy_max": C_DCA_MAX_BUY_PRICE,
                    "momentum_min": C_DCA_REBOUND_MOM_MIN,
                    "momentum_max": C_DCA_REBOUND_MOM_MAX,
                    "deadline_sec": DCA_DEADLINE_SEC,
                },
            },
            "J": {
                "entry": [SAFE_ENTRY_PRICE_MIN, SAFE_ENTRY_PRICE_MAX],
                "other_v2_votes_min": J_CONSENSUS_MIN_OTHER_TOKENS,
                "window_sec": CONSENSUS_WINDOW_SEC,
                "dca": False,
            },
        },
        "stop_loss": {
            "type": "fee_adjusted_profit_floor",
            "enabled": BREAKEVEN_STOP_ENABLED,
            "arm_move": BREAKEVEN_TRIGGER_MOVE,
            "target_profit_usdc": BREAKEVEN_MIN_PROFIT_USDC,
            "basis": "weighted gross entry average",
            "exit": "best-bid monitored paper liquidation",
        },
        "markets_tracked": len(markets),
        "assets_subscribed": len(subscribed_assets),
        "books": len(books),
        "memory_rss_mb": current_rss_mb(),
        "time_utc": utc_iso(),
    })


async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server on :%d", PORT)


async def main():
    global session
    init_db()
    session = aiohttp.ClientSession(headers={
        "User-Agent": f"M03Multi7ConsensusFGHJ/{VERSION}",
        "Accept": "application/json",
    })
    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(ws_loop()),
        asyncio.create_task(strategy_loop()),
        asyncio.create_task(resolution_fallback_loop()),
        asyncio.create_task(telegram_loop()),
        asyncio.create_task(memory_maintenance_loop()),
    ]
    log.info(
        "%s started | symbols=%s | accounts=%d | "
        "F=TIGHT+1V2 | G=TIGHT+2V2 | H=G+SAFE-DCA | J=WIDE+2V2 | "
        "window=%gs | BE-arm=+%.2f | profit-floor=$%.2f fee-adjusted | reports=OFF | trading=%s",
        VERSION, ",".join(SYMBOLS), len(STRATEGIES),
        CONSENSUS_WINDOW_SEC, BREAKEVEN_TRIGGER_MOVE, BREAKEVEN_MIN_PROFIT_USDC,
        "ON" if trading_enabled() else "OFF",
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if session:
            await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
