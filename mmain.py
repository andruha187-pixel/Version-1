import os
import io
import csv
import json
import time
import math
import zipfile
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
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.getenv("PORT", "8080"))

# We intentionally start with BTC only because our Powerwinner dataset
# is dominated by BTC 5-minute Up/Down markets.
SYMBOL = os.getenv("SYMBOL", "BTC").upper()

# The observed Powerwinner rhythm is about one strategy decision every 3 sec.
DECISION_INTERVAL = float(os.getenv("DECISION_INTERVAL", "3.0"))

# Stop opening new positions after this many seconds from market start.
TRADE_WINDOW_SECONDS = int(os.getenv("TRADE_WINDOW_SECONDS", "180"))

# Paper order size per signal. 10 shares keeps the simulation liquid and
# makes variants comparable. Scale later after finding profitable logic.
ORDER_SIZE = float(os.getenv("ORDER_SIZE", "10"))

# Crypto taker fee rate from current Polymarket docs.
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))

# Market discovery frequency.
DISCOVERY_INTERVAL = float(os.getenv("DISCOVERY_INTERVAL", "10"))

# Full book older than this triggers REST refresh before simulated execution.
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))

# Reports are sent 5 minutes after the hour closes.
REPORT_DELAY_SECONDS = int(os.getenv("REPORT_DELAY_SECONDS", "300"))
REPORT_CHECK_INTERVAL = int(os.getenv("REPORT_CHECK_INTERVAL", "30"))

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    p = DATA_DIR / ".write_test"
    p.write_text("ok")
    p.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "strategy_simulator.db"
REPORT_DIR = DATA_DIR / "strategy_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("strategy-sim")

session: Optional[aiohttp.ClientSession] = None

# ============================================================
# STRATEGY GRID
# ============================================================
#
# We do NOT pretend to know Powerwinner's exact formula yet.
# Run several candidate variants simultaneously on the same live books.
#
# entry_move:
#   minimum rise in ask price over lookback needed for first entry.
#
# pyramid_step:
#   after buying a side, it must rise this much above its previous buy price
#   before another fixed-size lot is added.
#
# lookback:
#   number of 3-sec samples used to measure momentum.
#
# switch_move:
#   minimum opposite-side momentum required to start buying the other side.
#
# max_buys_side:
#   maximum number of lots on each side per market.
#
# min_price/max_price:
#   avoid extreme contracts where movement has different behavior.
#

VARIANTS = [
    {"name": "M03_P08_L2", "entry_move": 0.03, "pyramid_step": 0.08, "lookback": 2, "switch_move": 0.04, "max_buys_side": 6},
    {"name": "M04_P08_L2", "entry_move": 0.04, "pyramid_step": 0.08, "lookback": 2, "switch_move": 0.04, "max_buys_side": 6},
    {"name": "M05_P08_L2", "entry_move": 0.05, "pyramid_step": 0.08, "lookback": 2, "switch_move": 0.05, "max_buys_side": 6},
    {"name": "M05_P10_L2", "entry_move": 0.05, "pyramid_step": 0.10, "lookback": 2, "switch_move": 0.05, "max_buys_side": 6},
    {"name": "M06_P10_L2", "entry_move": 0.06, "pyramid_step": 0.10, "lookback": 2, "switch_move": 0.06, "max_buys_side": 6},
    {"name": "M08_P10_L2", "entry_move": 0.08, "pyramid_step": 0.10, "lookback": 2, "switch_move": 0.08, "max_buys_side": 6},
    {"name": "M05_P10_L3", "entry_move": 0.05, "pyramid_step": 0.10, "lookback": 3, "switch_move": 0.05, "max_buys_side": 6},
    {"name": "M08_P12_L3", "entry_move": 0.08, "pyramid_step": 0.12, "lookback": 3, "switch_move": 0.08, "max_buys_side": 5},
]

MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))

# ============================================================
# SHARED MARKET STATE
# ============================================================

books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()

# price_history[condition][asset] -> deque [(timestamp_ms, ask)]
price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=100)))

# strategy_state[(condition, variant)] -> state dict
strategy_state = {}

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
    # Polymarket rounds fees to 5 decimals.
    return round(fee, 5) if fee >= 0.000005 else 0.0

def target_market_text(m):
    return f"{m.get('question','')} {m.get('slug','')}".lower()

def is_target_market(m):
    s = target_market_text(m)

    if SYMBOL == "BTC":
        symbol_ok = ("bitcoin" in s or "btc" in s)
    elif SYMBOL == "ETH":
        symbol_ok = ("ethereum" in s or "eth" in s)
    else:
        symbol_ok = True

    updown = ("up or down" in s or "up-down" in s)
    return (
        symbol_ok
        and updown
        and bool(m.get("enableOrderBook", True))
        and not bool(m.get("closed", False))
    )

# ============================================================
# DATABASE
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

        CREATE TABLE IF NOT EXISTS market_results (
            condition_id TEXT,
            variant TEXT,
            winning_asset TEXT,
            winning_outcome TEXT,
            total_cost REAL,
            payout REAL,
            pnl REAL,
            trades INTEGER,
            up_shares REAL,
            down_shares REAL,
            settled_ms INTEGER,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_trades_ms ON paper_trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_condition ON paper_trades(condition_id);
        CREATE INDEX IF NOT EXISTS idx_signals_ms ON signals(signal_ms);
        CREATE INDEX IF NOT EXISTS idx_results_ms ON market_results(settled_ms);
        """)

def state_get(key, default=None):
    with db() as conn:
        r = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

def state_set(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        conn.commit()

# ============================================================
# HTTP
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

# ============================================================
# BOOK
# ============================================================

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
        asset = str(
            ch.get("asset_id")
            or ch.get("token_id")
            or ch.get("tokenId")
            or ""
        )
        if not asset:
            continue

        b = books.setdefault(asset, {
            "bids": {},
            "asks": {},
            "received_ms": recv,
            "source": "ws-delta",
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
    if not b or not b["asks"]:
        return None
    return min(b["asks"])

async def refresh_book(asset):
    data = await get_json(f"{CLOB_API}/book", params={"token_id": asset})
    if isinstance(data, dict):
        apply_book(asset, data, "rest")
        return True
    return False

async def ensure_book(asset):
    b = books.get(asset)
    if b and b["asks"]:
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
    if not b or not b["asks"]:
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

# ============================================================
# MARKET DISCOVERY
# ============================================================

def persist_market(m):
    cid = m["condition_id"]
    with db() as conn:
        conn.execute("""
            INSERT INTO discovered_markets(
                condition_id, question, slug, start_ts, end_ts,
                up_asset, down_asset, discovered_ms
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                question=excluded.question,
                slug=excluded.slug,
                start_ts=excluded.start_ts,
                end_ts=excluded.end_ts,
                up_asset=excluded.up_asset,
                down_asset=excluded.down_asset
        """, (
            cid,
            m["question"],
            m["slug"],
            m["start_ts"],
            m["end_ts"],
            m["up_asset"],
            m["down_asset"],
            now_ms(),
        ))
        conn.commit()

async def subscribe_asset(asset):
    if not asset or asset in subscribed_assets:
        return

    subscribed_assets.add(asset)
    await ws_send_queue.put({
        "operation": "subscribe",
        "assets_ids": [asset],
    })

async def discovery_loop():
    """Robust discovery of short-lived BTC Up/Down markets."""
    while True:
        try:
            now_epoch = now_ts()
            found_this_pass = 0

            for offset in (0, 100, 200, 300, 400):
                data = await get_json(
                    f"{GAMMA_API}/markets",
                    params={
                        "limit": 100,
                        "offset": offset,
                        "active": "true",
                        "closed": "false",
                        "order": "endDate",
                        "ascending": "true",
                    },
                )
                if not isinstance(data, list):
                    continue
                if not data:
                    break

                for raw in data:
                    if not is_target_market(raw):
                        continue

                    outcomes = parse_jsonish(raw.get("outcomes"))
                    tokens = parse_jsonish(raw.get("clobTokenIds"))
                    if len(outcomes) != 2 or len(tokens) != 2:
                        continue

                    mapping = {str(outcomes[i]).strip().lower(): str(tokens[i]) for i in range(2)}
                    up_asset = mapping.get("up")
                    down_asset = mapping.get("down")
                    if not up_asset or not down_asset:
                        continue

                    end_dt = parse_iso(raw.get("endDate"))
                    if not end_dt:
                        continue
                    end_ts = int(end_dt.timestamp())
                    if end_ts < now_epoch - 60 or end_ts > now_epoch + 20 * 60:
                        continue

                    start_dt = parse_iso(raw.get("eventStartTime")) or parse_iso(raw.get("startDate"))
                    start_ts = int(start_dt.timestamp()) if start_dt else end_ts - 300
                    if end_ts - start_ts > 600 or end_ts <= start_ts:
                        start_ts = end_ts - 300

                    cid = str(raw.get("conditionId") or "")
                    if not cid:
                        continue

                    market = {
                        "condition_id": cid,
                        "question": str(raw.get("question") or ""),
                        "slug": str(raw.get("slug") or ""),
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "up_asset": up_asset,
                        "down_asset": down_asset,
                        "raw": raw,
                    }

                    if cid not in markets:
                        markets[cid] = market
                        persist_market(market)
                        await subscribe_asset(up_asset)
                        await subscribe_asset(down_asset)
                        found_this_pass += 1
                        log.info(
                            "MARKET %s | start=%s end=%s | assets=%s,%s",
                            market["question"], utc_iso(start_ts), utc_iso(end_ts),
                            up_asset[-8:], down_asset[-8:],
                        )

                last_end = parse_iso(data[-1].get("endDate")) if data else None
                if last_end and int(last_end.timestamp()) > now_epoch + 20 * 60:
                    break

            if found_this_pass == 0 and not markets:
                log.info("Discovery: no BTC 5M market found yet; retrying")

        except Exception:
            log.exception("Discovery loop failed")

        await asyncio.sleep(DISCOVERY_INTERVAL)

# ============================================================
# WEBSOCKET
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
                    async for raw in ws:
                        for ev in parse_ws(raw):
                            if not isinstance(ev, dict):
                                continue

                            et = str(ev.get("event_type") or ev.get("type") or "")
                            payload = (
                                ev.get("payload")
                                if isinstance(ev.get("payload"), dict)
                                else ev
                            )

                            if et == "book":
                                asset = str(
                                    payload.get("asset_id")
                                    or payload.get("token_id")
                                    or ""
                                )
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
# STRATEGY ENGINE
# ============================================================

def get_variant_state(condition, variant):
    key = (condition, variant["name"])

    if key not in strategy_state:
        strategy_state[key] = {
            "buys": defaultdict(int),
            "last_buy": {},
            "started_sides": set(),
            "last_signal_ms": 0,
        }

    return strategy_state[key]

def momentum_for(condition, asset, lookback):
    h = price_history[condition][asset]

    if len(h) <= lookback:
        return None, None

    current = h[-1][1]
    ref = h[-1 - lookback][1]

    return current - ref, ref

def store_signal(condition, variant, asset, outcome, ask, ref, mom, signal_type, elapsed):
    with db() as conn:
        conn.execute("""
            INSERT INTO signals(
                signal_ms, condition_id, variant, asset, outcome,
                ask, reference_ask, momentum, signal_type, elapsed_sec
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, variant["name"], asset, outcome,
            ask, ref, mom, signal_type, elapsed,
        ))
        conn.commit()

async def execute_paper(condition, variant, asset, outcome, signal_type):
    age = await ensure_book(asset)
    fills, filled = simulate_buy(asset, ORDER_SIZE)

    if filled <= 0:
        return False

    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    avg = gross / filled
    total = gross + fee

    with db() as conn:
        conn.execute("""
            INSERT INTO paper_trades(
                trade_ms, condition_id, variant, asset, outcome,
                signal_type, requested_shares, filled_shares,
                avg_price, gross_cost, fee, total_cost,
                book_age_ms, fills_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, variant["name"], asset, outcome,
            signal_type, ORDER_SIZE, filled,
            avg, gross, fee, total,
            age,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.commit()

    st = get_variant_state(condition, variant)
    st["buys"][asset] += 1
    st["last_buy"][asset] = avg
    st["started_sides"].add(asset)

    log.info(
        "%s %s %s %s %.1fsh @ %.4f fee=%.4f",
        variant["name"],
        signal_type,
        outcome,
        condition[-6:],
        filled,
        avg,
        fee,
    )

    return True

async def evaluate_variant(market, variant, elapsed):
    cid = market["condition_id"]

    sides = [
        (market["up_asset"], "Up"),
        (market["down_asset"], "Down"),
    ]

    st = get_variant_state(cid, variant)

    candidates = []

    for asset, outcome in sides:
        ask = best_ask(asset)

        if ask is None or ask < MIN_PRICE or ask > MAX_PRICE:
            continue

        mom, ref = momentum_for(cid, asset, variant["lookback"])
        if mom is None:
            continue

        buys = st["buys"][asset]
        other_asset = market["down_asset"] if asset == market["up_asset"] else market["up_asset"]

        signal = None

        if buys == 0:
            # First entry on this side.
            threshold = (
                variant["entry_move"]
                if not st["started_sides"]
                else variant["switch_move"]
            )

            if mom >= threshold:
                signal = "ENTRY" if not st["started_sides"] else "SWITCH"

        else:
            # Momentum pyramiding: add only ABOVE the last buy price.
            last_buy = st["last_buy"].get(asset)

            if (
                last_buy is not None
                and ask >= last_buy + variant["pyramid_step"]
                and mom > 0
                and buys < variant["max_buys_side"]
            ):
                signal = "PYRAMID"

        if signal:
            candidates.append((mom, asset, outcome, ask, ref, signal))

    # One decision / one order per variant per 3-second tick.
    # If both sides somehow qualify, choose stronger current momentum.
    if not candidates:
        return

    candidates.sort(reverse=True, key=lambda x: x[0])
    mom, asset, outcome, ask, ref, signal = candidates[0]

    store_signal(
        cid, variant, asset, outcome, ask, ref, mom, signal, elapsed
    )

    await execute_paper(cid, variant, asset, outcome, signal)

async def strategy_loop():
    # Align decisions roughly to 3-second cadence rather than drift.
    while True:
        started = time.monotonic()
        now = time.time()

        try:
            for cid, market in list(markets.items()):
                elapsed = now - market["start_ts"]

                # Record prices from shortly before open through resolution.
                if -30 <= elapsed <= 310:
                    for asset in (market["up_asset"], market["down_asset"]):
                        ask = best_ask(asset)
                        if ask is not None:
                            price_history[cid][asset].append((now_ms(), ask))

                if elapsed < 0 or elapsed > TRADE_WINDOW_SECONDS:
                    continue

                # Need both books to compare / switch reliably.
                if best_ask(market["up_asset"]) is None:
                    continue
                if best_ask(market["down_asset"]) is None:
                    continue

                for variant in VARIANTS:
                    await evaluate_variant(market, variant, elapsed)

        except Exception:
            log.exception("Strategy loop failed")

        spent = time.monotonic() - started
        await asyncio.sleep(max(0.05, DECISION_INTERVAL - spent))

# ============================================================
# RESOLUTION
# ============================================================

async def settle_from_resolution(ev):
    cid = str(ev.get("market") or ev.get("condition_id") or "")
    winning_asset = str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    winning_outcome = str(ev.get("winning_outcome") or "")

    if not cid or not winning_asset:
        return

    await settle_market(cid, winning_asset, winning_outcome)

async def settle_market(cid, winning_asset, winning_outcome):
    market = markets.get(cid)

    if not market:
        with db() as conn:
            row = conn.execute(
                "SELECT * FROM discovered_markets WHERE condition_id=?",
                (cid,),
            ).fetchone()
            if not row:
                return
            market = dict(row)

    with db() as conn:
        already = conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE condition_id=?",
            (cid,),
        ).fetchone()["c"]

        if already >= len(VARIANTS):
            return

        for variant in VARIANTS:
            exists = conn.execute(
                "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                (cid, variant["name"]),
            ).fetchone()

            if exists:
                continue

            rows = conn.execute("""
                SELECT * FROM paper_trades
                WHERE condition_id=? AND variant=?
            """, (cid, variant["name"])).fetchall()

            total_cost = sum(sf(r["total_cost"]) for r in rows)
            payout = sum(
                sf(r["filled_shares"])
                for r in rows
                if str(r["asset"]) == winning_asset
            )
            pnl = payout - total_cost

            up_asset = market["up_asset"]
            down_asset = market["down_asset"]

            up_shares = sum(
                sf(r["filled_shares"]) for r in rows if str(r["asset"]) == up_asset
            )
            down_shares = sum(
                sf(r["filled_shares"]) for r in rows if str(r["asset"]) == down_asset
            )

            conn.execute("""
                INSERT INTO market_results(
                    condition_id, variant, winning_asset, winning_outcome,
                    total_cost, payout, pnl, trades, up_shares,
                    down_shares, settled_ms
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                cid, variant["name"], winning_asset, winning_outcome,
                total_cost, payout, pnl, len(rows),
                up_shares, down_shares, now_ms(),
            ))

        conn.execute("""
            UPDATE discovered_markets
            SET resolved=1, winning_asset=?, winning_outcome=?
            WHERE condition_id=?
        """, (winning_asset, winning_outcome, cid))

        conn.commit()

    log.info(
        "RESOLVED %s | winner=%s",
        market.get("question", cid),
        winning_outcome or winning_asset[-8:],
    )

async def resolution_fallback_loop():
    """
    WebSocket market_resolved is preferred. Gamma fallback checks markets that
    ended but have not been settled locally.
    """
    while True:
        try:
            cutoff = now_ts() - 15

            with db() as conn:
                rows = conn.execute("""
                    SELECT condition_id
                    FROM discovered_markets
                    WHERE resolved=0 AND end_ts < ?
                    ORDER BY end_ts
                    LIMIT 30
                """, (cutoff,)).fetchall()

            for r in rows:
                cid = r["condition_id"]

                raw = await get_json(
                    f"{GAMMA_API}/markets",
                    params={"condition_ids": cid, "limit": 1},
                )

                if not isinstance(raw, list) or not raw:
                    continue

                m = raw[0]
                outcomes = parse_jsonish(m.get("outcomes"))
                prices = parse_jsonish(m.get("outcomePrices"))
                tokens = parse_jsonish(m.get("clobTokenIds"))

                if len(outcomes) != 2 or len(prices) != 2 or len(tokens) != 2:
                    continue

                ps = [sf(x, -1) for x in prices]

                if max(ps) < 0.999:
                    continue

                idx = 0 if ps[0] >= ps[1] else 1

                await settle_market(
                    cid,
                    str(tokens[idx]),
                    str(outcomes[idx]),
                )

        except Exception:
            log.exception("Resolution fallback failed")

        await asyncio.sleep(20)

# ============================================================
# HOURLY REPORT
# ============================================================

def csv_bytes(rows, columns=None):
    s = io.StringIO()

    if rows:
        if columns is None:
            columns = list(rows[0].keys())

        w = csv.DictWriter(s, fieldnames=columns, extrasaction="ignore")
        w.writeheader()

        for r in rows:
            w.writerow(dict(r))

    elif columns:
        w = csv.DictWriter(s, fieldnames=columns)
        w.writeheader()

    return s.getvalue().encode("utf-8-sig")

def variant_summary(start_ms, end_ms):
    out = []

    with db() as conn:
        for v in VARIANTS:
            rows = conn.execute("""
                SELECT *
                FROM market_results
                WHERE variant=? AND settled_ms>=? AND settled_ms<?
            """, (v["name"], start_ms, end_ms)).fetchall()

            pnl = sum(sf(r["pnl"]) for r in rows)
            cost = sum(sf(r["total_cost"]) for r in rows)
            wins = sum(1 for r in rows if sf(r["pnl"]) > 0)
            losses = sum(1 for r in rows if sf(r["pnl"]) < 0)

            trades = conn.execute("""
                SELECT COUNT(*) c
                FROM paper_trades
                WHERE variant=? AND trade_ms>=? AND trade_ms<?
            """, (v["name"], start_ms, end_ms)).fetchone()["c"]

            fees = conn.execute("""
                SELECT COALESCE(SUM(fee),0) f
                FROM paper_trades
                WHERE variant=? AND trade_ms>=? AND trade_ms<?
            """, (v["name"], start_ms, end_ms)).fetchone()["f"]

            out.append({
                "variant": v["name"],
                "entry_move": v["entry_move"],
                "pyramid_step": v["pyramid_step"],
                "lookback_ticks": v["lookback"],
                "switch_move": v["switch_move"],
                "max_buys_side": v["max_buys_side"],
                "markets_settled": len(rows),
                "winning_markets": wins,
                "losing_markets": losses,
                "paper_trades": trades,
                "fees": round(sf(fees), 5),
                "cost": round(cost, 5),
                "pnl": round(pnl, 5),
                "roi_pct": round((pnl / cost * 100) if cost > 0 else 0, 4),
            })

    return sorted(out, key=lambda x: x["pnl"], reverse=True)

def make_report(start_ts, end_ts):
    sm = start_ts * 1000
    em = end_ts * 1000

    with db() as conn:
        trades = conn.execute("""
            SELECT * FROM paper_trades
            WHERE trade_ms>=? AND trade_ms<?
            ORDER BY trade_ms
        """, (sm, em)).fetchall()

        signals = conn.execute("""
            SELECT * FROM signals
            WHERE signal_ms>=? AND signal_ms<?
            ORDER BY signal_ms
        """, (sm, em)).fetchall()

        results = conn.execute("""
            SELECT * FROM market_results
            WHERE settled_ms>=? AND settled_ms<?
            ORDER BY settled_ms, variant
        """, (sm, em)).fetchall()

        markets_rows = conn.execute("""
            SELECT * FROM discovered_markets
            WHERE discovered_ms<? AND end_ts>=?
            ORDER BY start_ts
        """, (em, start_ts - 300)).fetchall()

    summary = variant_summary(sm, em)

    lines = [
        "POWERWINNER-INSPIRED STRATEGY SIMULATOR v1",
        "=" * 70,
        f"Period UTC: {utc_iso(start_ts)} -> {utc_iso(end_ts)}",
        f"Symbol: {SYMBOL}",
        f"Decision interval: {DECISION_INTERVAL}s",
        f"Trading window: first {TRADE_WINDOW_SECONDS}s",
        f"Paper lot: {ORDER_SIZE} shares",
        "",
        "VARIANTS RANKED BY REALIZED PNL",
    ]

    for x in summary:
        lines.append(
            f"{x['variant']}: pnl=${x['pnl']:+.2f} | "
            f"ROI={x['roi_pct']:+.2f}% | markets={x['markets_settled']} | "
            f"W/L={x['winning_markets']}/{x['losing_markets']} | "
            f"trades={x['paper_trades']} | fees=${x['fees']:.2f}"
        )

    lines += [
        "",
        "IMPORTANT",
        "This is an independent paper strategy test. It does NOT copy Powerwinner.",
        "All fills use the live public order book and taker fees.",
        "No real orders are placed.",
    ]

    d1 = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    d2 = datetime.fromtimestamp(end_ts, tz=timezone.utc)

    path = REPORT_DIR / f"strategy_sim_{d1:%Y-%m-%d_%H-%M}_{d2:%H-%M}_UTC.zip"

    summary_cols = [
        "variant", "entry_move", "pyramid_step", "lookback_ticks",
        "switch_move", "max_buys_side", "markets_settled",
        "winning_markets", "losing_markets", "paper_trades",
        "fees", "cost", "pnl", "roi_pct"
    ]

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("variants_summary.csv", csv_bytes(summary, summary_cols))
        z.writestr("paper_trades.csv", csv_bytes(trades))
        z.writestr("signals.csv", csv_bytes(signals))
        z.writestr("market_results.csv", csv_bytes(results))
        z.writestr("markets.csv", csv_bytes(markets_rows))
        z.writestr("report.txt", "\n".join(lines).encode("utf-8"))

    return path, summary

async def tg_file(path, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured: %s", path)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", TELEGRAM_CHAT_ID)
        form.add_field("caption", caption[:1024])
        form.add_field(
            "document",
            path.read_bytes(),
            filename=path.name,
            content_type="application/zip",
        )

        async with session.post(
            url,
            data=form,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as r:
            if r.status != 200:
                log.warning("Telegram: %s", await r.text())
                return False
            return True

    except Exception:
        log.exception("Telegram send failed")
        return False

async def report_loop():
    saved = si(state_get("last_report_end", "0"))

    if saved <= 0:
        d = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        saved = int(d.timestamp())
        state_set("last_report_end", saved)

    last_end = saved

    while True:
        try:
            eligible = ((now_ts() - REPORT_DELAY_SECONDS) // 3600) * 3600

            while last_end < eligible:
                start = last_end
                end = start + 3600

                path, summary = make_report(start, end)

                best = summary[0] if summary else None

                if best:
                    extra = (
                        f"Best: {best['variant']} | "
                        f"PnL ${best['pnl']:+.2f} | ROI {best['roi_pct']:+.2f}%"
                    )
                else:
                    extra = "No settled markets yet"

                ok = await tg_file(
                    path,
                    (
                        "🧪 Strategy Simulator\n"
                        f"{utc_iso(start)} → {utc_iso(end)}\n"
                        f"{extra}"
                    ),
                )

                if not ok:
                    break

                last_end = end
                state_set("last_report_end", last_end)

        except Exception:
            log.exception("Report loop failed")

        await asyncio.sleep(REPORT_CHECK_INTERVAL)

# ============================================================
# HEALTH
# ============================================================

async def health(request):
    with db() as conn:
        t = conn.execute("SELECT COUNT(*) c FROM paper_trades").fetchone()["c"]
        r = conn.execute("SELECT COUNT(*) c FROM market_results").fetchone()["c"]
        p = conn.execute("SELECT COALESCE(SUM(pnl),0) p FROM market_results").fetchone()["p"]

    return web.json_response({
        "ok": True,
        "version": "1.1-discovery",
        "symbol": SYMBOL,
        "decision_interval": DECISION_INTERVAL,
        "trade_window_seconds": TRADE_WINDOW_SECONDS,
        "order_size": ORDER_SIZE,
        "variants": len(VARIANTS),
        "markets_tracked": len(markets),
        "assets_subscribed": len(subscribed_assets),
        "books": len(books),
        "paper_trades": t,
        "settled_variant_results": r,
        "aggregate_all_variant_pnl": p,
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

# ============================================================
# MAIN
# ============================================================

async def main():
    global session

    init_db()

    session = aiohttp.ClientSession(headers={
        "User-Agent": "PowerwinnerInspiredStrategySimulator/1.1",
        "Accept": "application/json",
    })

    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(ws_loop()),
        asyncio.create_task(strategy_loop()),
        asyncio.create_task(resolution_fallback_loop()),
        asyncio.create_task(report_loop()),
    ]

    log.info(
        "Strategy Simulator started | %d variants | %.1fs cycle | lot=%.1f",
        len(VARIANTS),
        DECISION_INTERVAL,
        ORDER_SIZE,
    )

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()

        if session:
            await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
