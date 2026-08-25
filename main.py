#!/usr/bin/env python3
"""
Polymarket Highest Temperature paper-trading bot.

IMPORTANT: this project NEVER sends real orders and requires no wallet/private key.
It only reads public Polymarket Gamma/CLOB data and simulates independent strategies.
"""

from __future__ import annotations

import asyncio
import csv
import html
import io
import json
import logging
import math
import os
import re
import sqlite3
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import aiohttp
from aiohttp import ClientSession, ClientTimeout, WSMsgType, web
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

UTC = timezone.utc
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def default_db_path() -> str:
    explicit = os.getenv("DB_PATH")
    if explicit:
        return explicit
    if Path("/var/data").is_dir():
        return "/var/data/weather_paper.db"
    return "weather_paper.db"


def parse_thresholds(raw: str | None) -> tuple[float, ...]:
    values: list[float] = []
    for part in (raw or "0.79,0.84,0.89").split(","):
        try:
            value = round(float(part.strip()), 6)
        except ValueError:
            continue
        if 0 < value < 1:
            values.append(value)
    if not values:
        values = [0.79, 0.84, 0.89]
    return tuple(sorted(set(values)))


THRESHOLDS = parse_thresholds(os.getenv("THRESHOLDS"))
TRADE_NOTIONAL_USD = env_float("TRADE_NOTIONAL_USD", 5.0)
START_BALANCE_USD = env_float("START_BALANCE_USD", 1000.0)
DISCOVERY_INTERVAL = env_int("DISCOVERY_INTERVAL_SECONDS", 300)
RESOLUTION_INTERVAL = env_int("RESOLUTION_INTERVAL_SECONDS", 60)
PRICE_SNAPSHOT_INTERVAL = env_int("PRICE_SNAPSHOT_INTERVAL_SECONDS", 30)
EQUITY_SNAPSHOT_INTERVAL = env_int("EQUITY_SNAPSHOT_INTERVAL_SECONDS", 60)
HOURLY_REPORTS = env_bool("HOURLY_REPORTS", True)
REQUIRE_FULL_FILL = env_bool("REQUIRE_FULL_FILL", True)
MIN_FULL_FILL_RATIO = env_float("MIN_FULL_FILL_RATIO", 0.999)
ENTRY_LIQUIDITY_CHECK_ENABLED = env_bool("ENTRY_LIQUIDITY_CHECK_ENABLED", True)
MIN_ENTRY_LIQUIDITY_USD = max(0.0, env_float("MIN_ENTRY_LIQUIDITY_USD", TRADE_NOTIONAL_USD))
MAX_ENTRY_SLIPPAGE = max(0.0, env_float("MAX_ENTRY_SLIPPAGE", 0.02))
WEATHER_FEE_RATE_FALLBACK = env_float("WEATHER_FEE_RATE_FALLBACK", 0.05)
STOP_LOSS_ENABLED = env_bool("STOP_LOSS_ENABLED", True)
STOP_LOSS_PRICE = min(0.999, max(0.001, env_float("STOP_LOSS_PRICE", 0.40)))
STOP_LOSS_SCAN_INTERVAL = max(1, env_int("STOP_LOSS_SCAN_INTERVAL_SECONDS", 5))
TRADING_ENABLED_ON_START = env_bool("TRADING_ENABLED_ON_START", True)
PORT = env_int("PORT", 8080)
DB_PATH = default_db_path()
REPORT_DIR = os.getenv("REPORT_DIR") or str(Path(DB_PATH).parent / "reports")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DISCOVERY_TAG_SLUGS = tuple(
    x.strip() for x in os.getenv("DISCOVERY_TAG_SLUGS", "temperature,weather").split(",") if x.strip()
)
HTTP_TIMEOUT = ClientTimeout(total=20, connect=10, sock_read=15)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)sZ [%(levelname)s] %(message)s",
)
logging.Formatter.converter = time.gmtime
log = logging.getLogger("weather-paper")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def parse_jsonish(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict, tuple)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return default
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


def as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        result = float(value)
        if not math.isfinite(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def fmt_money(value: float | None) -> str:
    if value is None:
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}${value:,.2f}"


def strategy_name(threshold: float) -> str:
    return f"T{int(round(threshold * 100)):02d}"


def is_temperature_event(event: dict[str, Any]) -> bool:
    title = str(event.get("title") or "").strip().lower()
    slug = str(event.get("slug") or "").strip().lower()
    return title.startswith("highest temperature in ") or slug.startswith("highest-temperature-in-")


def yes_token_from_market(market: dict[str, Any]) -> tuple[str | None, str | None]:
    outcomes = parse_jsonish(market.get("outcomes"), []) or []
    token_ids = parse_jsonish(market.get("clobTokenIds"), []) or []
    if not isinstance(outcomes, (list, tuple)) or not isinstance(token_ids, (list, tuple)):
        return None, None
    yes_token = no_token = None
    for i, outcome in enumerate(outcomes):
        if i >= len(token_ids):
            break
        name = str(outcome).strip().lower()
        if name == "yes":
            yes_token = str(token_ids[i])
        elif name == "no":
            no_token = str(token_ids[i])
    return yes_token, no_token


def extract_fee_info(market: dict[str, Any]) -> tuple[bool | None, float | None]:
    enabled_raw = market.get("feesEnabled")
    enabled: bool | None
    if isinstance(enabled_raw, bool):
        enabled = enabled_raw
    else:
        enabled = None

    schedule = market.get("feeSchedule") or market.get("fee_schedule")
    if isinstance(schedule, str):
        schedule = parse_jsonish(schedule, {})
    rate = None
    if isinstance(schedule, dict):
        rate = as_float(schedule.get("rate") if "rate" in schedule else schedule.get("r"))
    return enabled, rate


def market_temperature_label(market: dict[str, Any]) -> str:
    for key in ("groupItemTitle", "groupItemThreshold", "question"):
        value = market.get(key)
        if value:
            return str(value)
    return str(market.get("slug") or market.get("id") or "unknown")


@dataclass(slots=True)
class MarketInfo:
    market_id: str
    event_id: str
    condition_id: str
    event_slug: str
    event_title: str
    market_slug: str
    question: str
    temperature_label: str
    end_date: str | None
    yes_token_id: str
    no_token_id: str | None
    fees_enabled: bool | None = None
    fee_rate: float | None = None


@dataclass(slots=True)
class TopOfBook:
    best_bid: float | None = None
    best_ask: float | None = None
    timestamp_ms: int | None = None


@dataclass(slots=True)
class FillResult:
    requested_notional: float
    filled_notional: float
    shares: float
    avg_price: float | None
    levels: list[dict[str, float]] = field(default_factory=list)

    @property
    def full_fill_ratio(self) -> float:
        if self.requested_notional <= 0:
            return 0.0
        return self.filled_notional / self.requested_notional


@dataclass(slots=True)
class SellFillResult:
    requested_shares: float
    sold_shares: float
    gross_proceeds: float
    avg_price: float | None
    levels: list[dict[str, float]] = field(default_factory=list)

    @property
    def full_fill_ratio(self) -> float:
        if self.requested_shares <= 0:
            return 0.0
        return self.sold_shares / self.requested_shares


# -----------------------------------------------------------------------------
# SQLite storage
# -----------------------------------------------------------------------------


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.init_schema()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
                market_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                condition_id TEXT,
                event_slug TEXT,
                event_title TEXT,
                market_slug TEXT,
                question TEXT,
                temperature_label TEXT,
                end_date TEXT,
                yes_token_id TEXT NOT NULL,
                no_token_id TEXT,
                fees_enabled INTEGER,
                fee_rate REAL,
                active INTEGER NOT NULL DEFAULT 1,
                closed INTEGER NOT NULL DEFAULT 0,
                discovered_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                resolved_at TEXT,
                yes_resolution_value REAL
            );

            CREATE INDEX IF NOT EXISTS idx_markets_yes_token ON markets(yes_token_id);
            CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);
            CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id);

            CREATE TABLE IF NOT EXISTS top_prices (
                market_id TEXT PRIMARY KEY,
                yes_token_id TEXT NOT NULL,
                best_bid REAL,
                best_ask REAL,
                timestamp_ms INTEGER,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                threshold REAL NOT NULL,
                market_id TEXT NOT NULL,
                signal_time TEXT NOT NULL,
                previous_ask REAL,
                trigger_ask REAL,
                action TEXT NOT NULL,
                reason TEXT,
                trade_id INTEGER,
                UNIQUE(strategy, market_id)
            );

            CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(signal_time);

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                threshold REAL NOT NULL,
                market_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_title TEXT,
                event_slug TEXT,
                temperature_label TEXT,
                yes_token_id TEXT NOT NULL,
                signal_time TEXT NOT NULL,
                fill_time TEXT NOT NULL,
                previous_ask REAL,
                trigger_ask REAL,
                requested_notional REAL NOT NULL,
                filled_notional REAL NOT NULL,
                taker_fee REAL NOT NULL DEFAULT 0,
                cash_debit REAL NOT NULL,
                shares REAL NOT NULL,
                avg_fill_price REAL NOT NULL,
                best_bid_at_fill REAL,
                best_ask_at_fill REAL,
                fill_levels_json TEXT,
                status TEXT NOT NULL DEFAULT 'OPEN',
                resolution_value REAL,
                resolution_outcome TEXT,
                payout REAL,
                pnl REAL,
                resolution_source TEXT,
                resolved_at TEXT,
                redeemed_at TEXT,
                exit_reason TEXT,
                exit_time TEXT,
                exit_price REAL,
                exit_gross_proceeds REAL,
                exit_fee REAL NOT NULL DEFAULT 0,
                exit_net_proceeds REAL,
                exit_levels_json TEXT,
                stop_trigger_bid REAL,
                UNIQUE(strategy, market_id)
            );

            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy);
            CREATE INDEX IF NOT EXISTS idx_trades_event ON trades(event_id);
            CREATE INDEX IF NOT EXISTS idx_trades_fill_time ON trades(fill_time);

            CREATE TABLE IF NOT EXISTS equity_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                cash REAL NOT NULL,
                open_value REAL NOT NULL,
                equity REAL NOT NULL,
                realized_pnl REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                total_pnl REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_equity_strategy_time
                ON equity_snapshots(strategy, timestamp);

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        # Forward-compatible migration for databases created by the first paper-bot version.
        existing_trade_cols = {str(r[1]) for r in self.conn.execute("PRAGMA table_info(trades)").fetchall()}
        migrations = {
            "exit_reason": "TEXT",
            "exit_time": "TEXT",
            "exit_price": "REAL",
            "exit_gross_proceeds": "REAL",
            "exit_fee": "REAL NOT NULL DEFAULT 0",
            "exit_net_proceeds": "REAL",
            "exit_levels_json": "TEXT",
            "stop_trigger_bid": "REAL",
        }
        for col, ddl in migrations.items():
            if col not in existing_trade_cols:
                self.conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {ddl}")
        self.conn.commit()

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else None

    def upsert_market(self, m: MarketInfo) -> None:
        now = iso_now()
        self.conn.execute(
            """
            INSERT INTO markets(
                market_id,event_id,condition_id,event_slug,event_title,market_slug,question,
                temperature_label,end_date,yes_token_id,no_token_id,fees_enabled,fee_rate,
                active,closed,discovered_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?)
            ON CONFLICT(market_id) DO UPDATE SET
                event_id=excluded.event_id,
                condition_id=excluded.condition_id,
                event_slug=excluded.event_slug,
                event_title=excluded.event_title,
                market_slug=excluded.market_slug,
                question=excluded.question,
                temperature_label=excluded.temperature_label,
                end_date=excluded.end_date,
                yes_token_id=excluded.yes_token_id,
                no_token_id=excluded.no_token_id,
                fees_enabled=COALESCE(excluded.fees_enabled, markets.fees_enabled),
                fee_rate=COALESCE(excluded.fee_rate, markets.fee_rate),
                active=1,
                updated_at=excluded.updated_at
            """,
            (
                m.market_id,
                m.event_id,
                m.condition_id,
                m.event_slug,
                m.event_title,
                m.market_slug,
                m.question,
                m.temperature_label,
                m.end_date,
                m.yes_token_id,
                m.no_token_id,
                None if m.fees_enabled is None else int(m.fees_enabled),
                m.fee_rate,
                now,
                now,
            ),
        )
        self.conn.commit()

    def load_markets(self) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            """
            SELECT * FROM markets
            WHERE closed=0 AND (active=1 OR market_id IN (SELECT DISTINCT market_id FROM trades WHERE status='OPEN'))
            """
        ))

    def deactivate_missing_markets(self, seen_market_ids: set[str]) -> None:
        if not seen_market_ids:
            return
        placeholders = ",".join("?" for _ in seen_market_ids)
        self.conn.execute(
            f"UPDATE markets SET active=0, updated_at=? WHERE closed=0 AND market_id NOT IN ({placeholders})",
            (iso_now(), *sorted(seen_market_ids)),
        )
        self.conn.commit()

    def mark_market_resolved(self, market_id: str, yes_value: float, resolved_at: str) -> None:
        self.conn.execute(
            """
            UPDATE markets
            SET active=0, closed=1, yes_resolution_value=?, resolved_at=?, updated_at=?
            WHERE market_id=?
            """,
            (yes_value, resolved_at, iso_now(), market_id),
        )
        self.conn.commit()

    def save_top_price(self, market_id: str, token_id: str, top: TopOfBook) -> None:
        self.conn.execute(
            """
            INSERT INTO top_prices(market_id,yes_token_id,best_bid,best_ask,timestamp_ms,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(market_id) DO UPDATE SET
                yes_token_id=excluded.yes_token_id,
                best_bid=excluded.best_bid,
                best_ask=excluded.best_ask,
                timestamp_ms=excluded.timestamp_ms,
                updated_at=excluded.updated_at
            """,
            (market_id, token_id, top.best_bid, top.best_ask, top.timestamp_ms, iso_now()),
        )

    def flush(self) -> None:
        self.conn.commit()

    def load_signal_keys(self) -> set[tuple[str, str]]:
        rows = self.conn.execute("SELECT strategy, market_id FROM signals").fetchall()
        return {(str(r["strategy"]), str(r["market_id"])) for r in rows}

    def insert_signal(
        self,
        strategy: str,
        threshold: float,
        market_id: str,
        previous_ask: float,
        trigger_ask: float,
    ) -> int | None:
        try:
            cur = self.conn.execute(
                """
                INSERT INTO signals(strategy,threshold,market_id,signal_time,previous_ask,trigger_ask,action)
                VALUES(?,?,?,?,?,?,?)
                """,
                (strategy, threshold, market_id, iso_now(), previous_ask, trigger_ask, "PENDING"),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            return None

    def update_signal(self, signal_id: int, action: str, reason: str | None = None, trade_id: int | None = None) -> None:
        self.conn.execute(
            "UPDATE signals SET action=?, reason=?, trade_id=? WHERE id=?",
            (action, reason, trade_id, signal_id),
        )
        self.conn.commit()

    def insert_trade(self, values: dict[str, Any]) -> int:
        cols = [
            "strategy", "threshold", "market_id", "event_id", "event_title", "event_slug",
            "temperature_label", "yes_token_id", "signal_time", "fill_time", "previous_ask",
            "trigger_ask", "requested_notional", "filled_notional", "taker_fee", "cash_debit",
            "shares", "avg_fill_price", "best_bid_at_fill", "best_ask_at_fill", "fill_levels_json",
        ]
        placeholders = ",".join("?" for _ in cols)
        cur = self.conn.execute(
            f"INSERT INTO trades({','.join(cols)}) VALUES({placeholders})",
            tuple(values.get(c) for c in cols),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def settle_trades(
        self,
        market_id: str,
        yes_value: float,
        outcome: str,
        source: str,
        resolved_at: str,
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM trades WHERE market_id=? AND status='OPEN'",
            (market_id,),
        ).fetchall()
        settled: list[dict[str, Any]] = []
        for row in rows:
            payout = float(row["shares"]) * yes_value
            pnl = payout - float(row["cash_debit"])
            self.conn.execute(
                """
                UPDATE trades
                SET status='REDEEMED', resolution_value=?, resolution_outcome=?, payout=?, pnl=?,
                    resolution_source=?, resolved_at=?, redeemed_at=?
                WHERE id=?
                """,
                (yes_value, outcome, payout, pnl, source, resolved_at, iso_now(), row["id"]),
            )
            data = dict(row)
            data.update({"payout": payout, "pnl": pnl, "resolution_value": yes_value, "resolution_outcome": outcome})
            settled.append(data)
        self.conn.commit()
        return settled

    def close_stop_loss(
        self,
        trade_id: int,
        trigger_bid: float,
        sell: SellFillResult,
        exit_fee: float,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM trades WHERE id=? AND status='OPEN'",
            (trade_id,),
        ).fetchone()
        if row is None:
            return None
        net = float(sell.gross_proceeds) - float(exit_fee)
        pnl = net - float(row["cash_debit"])
        now = iso_now()
        self.conn.execute(
            """
            UPDATE trades
            SET status='STOP_LOSS', payout=?, pnl=?, exit_reason='STOP_LOSS', exit_time=?,
                exit_price=?, exit_gross_proceeds=?, exit_fee=?, exit_net_proceeds=?,
                exit_levels_json=?, stop_trigger_bid=?
            WHERE id=? AND status='OPEN'
            """,
            (
                net, pnl, now, sell.avg_price, sell.gross_proceeds, exit_fee, net,
                json.dumps(sell.levels, separators=(",", ":")), trigger_bid, trade_id,
            ),
        )
        self.conn.commit()
        data = dict(row)
        data.update({
            "status": "STOP_LOSS", "payout": net, "pnl": pnl, "exit_time": now,
            "exit_price": sell.avg_price, "exit_gross_proceeds": sell.gross_proceeds,
            "exit_fee": exit_fee, "exit_net_proceeds": net, "stop_trigger_bid": trigger_bid,
        })
        return data

    def free_cash(self, strategy: str) -> float:
        row = self.conn.execute(
            """
            SELECT
              COALESCE(SUM(cash_debit),0) AS debits,
              COALESCE(SUM(CASE WHEN status IN ('REDEEMED','STOP_LOSS') THEN payout ELSE 0 END),0) AS payouts
            FROM trades WHERE strategy=?
            """,
            (strategy,),
        ).fetchone()
        return START_BALANCE_USD - float(row["debits"]) + float(row["payouts"])

    def open_trades(self, strategy: str | None = None) -> list[sqlite3.Row]:
        if strategy:
            return list(self.conn.execute("SELECT * FROM trades WHERE status='OPEN' AND strategy=? ORDER BY fill_time", (strategy,)))
        return list(self.conn.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY fill_time"))

    def open_trades_by_token(self, token_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM trades WHERE status='OPEN' AND yes_token_id=? ORDER BY id",
            (token_id,),
        ))

    def open_market_ids(self) -> list[str]:
        rows = self.conn.execute("SELECT DISTINCT market_id FROM trades WHERE status='OPEN'").fetchall()
        return [str(r[0]) for r in rows]

    def latest_trades(self, limit: int = 30) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)))

    def market_row(self, market_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM markets WHERE market_id=?", (market_id,)).fetchone()

    def active_counts(self) -> tuple[int, int]:
        row = self.conn.execute(
            "SELECT COUNT(*) AS markets, COUNT(DISTINCT event_id) AS events FROM markets WHERE active=1 AND closed=0"
        ).fetchone()
        return int(row["events"]), int(row["markets"])

    def total_market_count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0])

    def stats(self, strategy: str, marks: dict[str, TopOfBook]) -> dict[str, float | int | str]:
        row = self.conn.execute(
            """
            SELECT
              COUNT(*) AS trades,
              SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_count,
              SUM(CASE WHEN status IN ('REDEEMED','STOP_LOSS') THEN 1 ELSE 0 END) AS resolved_count,
              SUM(CASE WHEN status='STOP_LOSS' THEN 1 ELSE 0 END) AS stop_loss_count,
              SUM(CASE WHEN status IN ('REDEEMED','STOP_LOSS') AND pnl > 0 THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN status IN ('REDEEMED','STOP_LOSS') AND pnl < 0 THEN 1 ELSE 0 END) AS losses,
              COALESCE(SUM(CASE WHEN status IN ('REDEEMED','STOP_LOSS') THEN pnl ELSE 0 END),0) AS realized_pnl,
              COALESCE(SUM(taker_fee + COALESCE(exit_fee,0)),0) AS fees,
              COALESCE(AVG(avg_fill_price),0) AS avg_entry
            FROM trades WHERE strategy=?
            """,
            (strategy,),
        ).fetchone()
        open_rows = self.open_trades(strategy)
        open_value = 0.0
        open_cost = 0.0
        for tr in open_rows:
            top = marks.get(str(tr["yes_token_id"]))
            mark = top.best_bid if top and top.best_bid is not None else float(tr["avg_fill_price"])
            open_value += float(tr["shares"]) * mark
            open_cost += float(tr["cash_debit"])
        cash = self.free_cash(strategy)
        unrealized = open_value - open_cost
        equity = cash + open_value
        total_pnl = equity - START_BALANCE_USD
        resolved = int(row["resolved_count"] or 0)
        wins = int(row["wins"] or 0)
        win_rate = (wins / resolved * 100.0) if resolved else 0.0
        max_dd = self.max_drawdown(strategy)
        return {
            "strategy": strategy,
            "threshold": int(strategy.removeprefix("T")) / 100.0 if strategy.startswith("T") else 0.0,
            "cash": cash,
            "open_value": open_value,
            "equity": equity,
            "realized_pnl": float(row["realized_pnl"] or 0),
            "unrealized_pnl": unrealized,
            "total_pnl": total_pnl,
            "roi_pct": (total_pnl / START_BALANCE_USD * 100.0) if START_BALANCE_USD else 0.0,
            "trades": int(row["trades"] or 0),
            "open_count": int(row["open_count"] or 0),
            "resolved_count": resolved,
            "stop_loss_count": int(row["stop_loss_count"] or 0),
            "wins": wins,
            "losses": int(row["losses"] or 0),
            "win_rate": win_rate,
            "fees": float(row["fees"] or 0),
            "avg_entry": float(row["avg_entry"] or 0),
            "max_drawdown_pct": max_dd,
        }

    def snapshot_equity(self, strategy: str, stat: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO equity_snapshots(strategy,timestamp,cash,open_value,equity,realized_pnl,unrealized_pnl,total_pnl)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                strategy, iso_now(), stat["cash"], stat["open_value"], stat["equity"],
                stat["realized_pnl"], stat["unrealized_pnl"], stat["total_pnl"],
            ),
        )
        self.conn.commit()

    def max_drawdown(self, strategy: str) -> float:
        rows = self.conn.execute(
            "SELECT equity FROM equity_snapshots WHERE strategy=? ORDER BY id",
            (strategy,),
        ).fetchall()
        peak = START_BALANCE_USD
        max_dd = 0.0
        for r in rows:
            equity = float(r["equity"])
            peak = max(peak, equity)
            if peak > 0:
                dd = (peak - equity) / peak * 100.0
                max_dd = max(max_dd, dd)
        return max_dd

    def export_rows(self, table: str, where: str = "", params: tuple[Any, ...] = ()) -> tuple[list[str], list[sqlite3.Row]]:
        allowed = {"markets", "trades", "signals", "equity_snapshots", "top_prices"}
        if table not in allowed:
            raise ValueError("Unsupported table")
        query = f"SELECT * FROM {table}"
        if where:
            query += " WHERE " + where
        rows = list(self.conn.execute(query, params))
        columns = [d[0] for d in self.conn.execute(query + " LIMIT 0", params).description]
        return columns, rows


# -----------------------------------------------------------------------------
# Telegram client
# -----------------------------------------------------------------------------


class TelegramClient:
    def __init__(self, session: ClientSession) -> None:
        self.session = session
        self.token = TELEGRAM_BOT_TOKEN
        self.default_chat_id = TELEGRAM_CHAT_ID
        self.offset = 0

    @property
    def enabled(self) -> bool:
        return bool(self.token)

    def api_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.token}/{method}"

    async def send_message(
        self,
        text: str,
        chat_id: str | int | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        target = chat_id if chat_id is not None else self.default_chat_id
        if not self.enabled or target is None:
            return
        payload: dict[str, Any] = {
            "chat_id": target,
            "text": text[:4096],
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        try:
            async with self.session.post(
                self.api_url("sendMessage"),
                json=payload,
                timeout=HTTP_TIMEOUT,
            ) as resp:
                if resp.status >= 300:
                    log.warning("Telegram sendMessage failed: %s %s", resp.status, (await resp.text())[:300])
        except Exception as exc:
            log.warning("Telegram sendMessage error: %s", exc)

    async def send_document(self, path: str, caption: str = "", chat_id: str | int | None = None) -> None:
        target = chat_id if chat_id is not None else self.default_chat_id
        if not self.enabled or target is None:
            return
        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(target))
            if caption:
                form.add_field("caption", caption[:1024])
            with open(path, "rb") as fh:
                form.add_field("document", fh, filename=Path(path).name, content_type="application/zip")
                async with self.session.post(self.api_url("sendDocument"), data=form, timeout=ClientTimeout(total=60)) as resp:
                    if resp.status >= 300:
                        log.warning("Telegram sendDocument failed: %s %s", resp.status, (await resp.text())[:300])
        except Exception as exc:
            log.warning("Telegram sendDocument error: %s", exc)

    async def get_updates(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        payload = {"offset": self.offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]}
        try:
            async with self.session.get(
                self.api_url("getUpdates"), params=payload, timeout=ClientTimeout(total=35)
            ) as resp:
                data = await resp.json(content_type=None)
                if not data.get("ok"):
                    return []
                results = data.get("result") or []
                if results:
                    self.offset = max(int(x.get("update_id", 0)) for x in results) + 1
                return results
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("Telegram getUpdates error: %s", exc)
            return []

    async def answer_callback(self, callback_id: str, text: str = "") -> None:
        if not self.enabled or not callback_id:
            return
        try:
            async with self.session.post(
                self.api_url("answerCallbackQuery"),
                json={"callback_query_id": callback_id, "text": text[:200]},
                timeout=HTTP_TIMEOUT,
            ) as resp:
                if resp.status >= 300:
                    log.debug("Telegram answerCallbackQuery failed: %s", resp.status)
        except Exception as exc:
            log.debug("Telegram callback answer error: %s", exc)


# -----------------------------------------------------------------------------
# Main bot
# -----------------------------------------------------------------------------


class WeatherPaperBot:
    def __init__(self) -> None:
        self.db = Database(DB_PATH)
        self.session: ClientSession | None = None
        self.telegram: TelegramClient | None = None
        self.markets_by_yes: dict[str, MarketInfo] = {}
        self.markets_by_id: dict[str, MarketInfo] = {}
        self.markets_by_condition: dict[str, MarketInfo] = {}
        self.tops: dict[str, TopOfBook] = {}
        self.books: dict[str, dict[str, dict[float, float]]] = {}
        self.last_asks: dict[str, float] = {}
        self.signal_keys: set[tuple[str, str]] = self.db.load_signal_keys()
        self.fee_rate_cache: dict[str, float] = {}
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        self.last_ws_message_at: str | None = None
        self.last_discovery_at: str | None = None
        self.last_resolution_scan_at: str | None = None
        self.dynamic_subscribe_queue: asyncio.Queue[str] = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.pending_crossings: set[str] = set()
        self.pending_stop_losses: set[int] = set()
        # Resolved markets can continue emitting terminal CLOB prices briefly.
        # Never allow those prices to create a new paper entry/stop.
        self.closed_market_ids: set[str] = set()
        saved_trading_state = self.db.get_meta("trading_enabled")
        self.trading_enabled = (
            TRADING_ENABLED_ON_START if saved_trading_state is None else saved_trading_state == "1"
        )

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        self.session = ClientSession(timeout=HTTP_TIMEOUT, connector=connector, headers={"User-Agent": "polymarket-weather-paper-bot/2.2"})
        self.telegram = TelegramClient(self.session)
        self.load_cached_markets()
        await self.discover_markets()
        await self.resolve_open_positions_once()

        tasks = [
            asyncio.create_task(self.discovery_loop(), name="discovery"),
            asyncio.create_task(self.websocket_loop(), name="websocket"),
            asyncio.create_task(self.resolution_loop(), name="resolution"),
            asyncio.create_task(self.stop_loss_scan_loop(), name="stop-loss-scan"),
            asyncio.create_task(self.price_snapshot_loop(), name="price-snapshots"),
            asyncio.create_task(self.equity_snapshot_loop(), name="equity-snapshots"),
            asyncio.create_task(self.telegram_loop(), name="telegram"),
        ]
        if HOURLY_REPORTS:
            tasks.append(asyncio.create_task(self.hourly_report_loop(), name="hourly-report"))

        events, markets = self.db.active_counts()
        startup = (
            "🌡 Polymarket Weather PAPER bot started\n"
            f"Strategies: {', '.join(f'{t:.2f}' for t in THRESHOLDS)}\n"
            f"Paper order: ${TRADE_NOTIONAL_USD:.2f} each\n"
            f"Balance: ${START_BALANCE_USD:.2f} per strategy\n"
            f"Entry liquidity: ${MIN_ENTRY_LIQUIDITY_USD:.2f} within +${MAX_ENTRY_SLIPPAGE:.2f} of best ask\n"
            f"Tracked city/date events: {events}\n"
            f"Tracked YES markets: {markets}\n"
            "REAL TRADING: disabled"
        )
        log.info(startup.replace("\n", " | "))
        if self.telegram:
            await self.telegram.send_message(startup, reply_markup=self.telegram_keyboard())

        try:
            await self.stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.session:
                await self.session.close()

    @staticmethod
    def telegram_keyboard() -> dict[str, Any]:
        return {
            "inline_keyboard": [
                [
                    {"text": "▶️ Старт", "callback_data": "paper_start"},
                    {"text": "⏹ Стоп", "callback_data": "paper_stop"},
                ],
                [
                    {"text": "📂 Позиции", "callback_data": "paper_positions"},
                    {"text": "📊 Отчёт", "callback_data": "paper_report"},
                ],
            ]
        }

    def set_trading_enabled(self, enabled: bool) -> None:
        self.trading_enabled = bool(enabled)
        self.db.set_meta("trading_enabled", "1" if enabled else "0")

    def load_cached_markets(self) -> None:
        for row in self.db.load_markets():
            m = MarketInfo(
                market_id=str(row["market_id"]),
                event_id=str(row["event_id"]),
                condition_id=str(row["condition_id"] or ""),
                event_slug=str(row["event_slug"] or ""),
                event_title=str(row["event_title"] or ""),
                market_slug=str(row["market_slug"] or ""),
                question=str(row["question"] or ""),
                temperature_label=str(row["temperature_label"] or ""),
                end_date=row["end_date"],
                yes_token_id=str(row["yes_token_id"]),
                no_token_id=str(row["no_token_id"]) if row["no_token_id"] else None,
                fees_enabled=None if row["fees_enabled"] is None else bool(row["fees_enabled"]),
                fee_rate=as_float(row["fee_rate"]),
            )
            self.register_market_memory(m)

    def register_market_memory(self, m: MarketInfo) -> bool:
        is_new = m.yes_token_id not in self.markets_by_yes
        self.markets_by_yes[m.yes_token_id] = m
        self.markets_by_id[m.market_id] = m
        if m.condition_id:
            self.markets_by_condition[m.condition_id.lower()] = m
        return is_new

    async def gamma_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        assert self.session is not None
        url = GAMMA_BASE + path
        async with self.session.get(url, params=params) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise RuntimeError(f"Gamma {resp.status}: {text[:250]}")
            return await resp.json(content_type=None)

    async def clob_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        assert self.session is not None
        url = CLOB_BASE + path
        async with self.session.get(url, params=params) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise RuntimeError(f"CLOB {resp.status}: {text[:250]}")
            return await resp.json(content_type=None)

    async def market_accepting_orders(self, m: MarketInfo) -> bool:
        """Reject an entry/stop only when Gamma explicitly says the market is not tradable.

        Gamma can briefly lag during resolution, so network/unknown status falls back to
        True; terminal $1 websocket quotes are separately blocked in handle_top.
        """
        if m.market_id in self.closed_market_ids:
            return False
        try:
            market = await self.gamma_get(f"/markets/{m.market_id}")
        except Exception as exc:
            log.debug("Market status verification failed market=%s: %s", m.market_id, exc)
            return True
        if not isinstance(market, dict):
            return True
        if market.get("closed") is True:
            return False
        if market.get("active") is False:
            return False
        if market.get("acceptingOrders") is False or market.get("accepting_orders") is False:
            return False
        if market.get("enableOrderBook") is False or market.get("enable_order_book") is False:
            return False
        return True

    async def discover_markets(self) -> int:
        events: dict[str, dict[str, Any]] = {}
        # Primary discovery: official events endpoint filtered by temperature/weather tags.
        for tag_slug in DISCOVERY_TAG_SLUGS:
            offset = 0
            for _ in range(20):
                params = {
                    "limit": 100,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "tag_slug": tag_slug,
                    "order": "endDate",
                    "ascending": "true",
                }
                try:
                    batch = await self.gamma_get("/events", params)
                except Exception as exc:
                    log.warning("Discovery tag=%s failed: %s", tag_slug, exc)
                    break
                if not isinstance(batch, list):
                    break
                for event in batch:
                    if isinstance(event, dict) and is_temperature_event(event):
                        events[str(event.get("id") or event.get("slug"))] = event
                if len(batch) < 100:
                    break
                offset += len(batch)

        # Secondary discovery: union with public search in case tag taxonomy changes.
        page = 1
        for _ in range(20):
            params = {
                "q": "highest temperature in",
                "events_status": "active",
                "limit_per_type": 50,
                "page": page,
                "keep_closed_markets": 0,
                "search_tags": "false",
                "search_profiles": "false",
            }
            try:
                data = await self.gamma_get("/public-search", params)
            except Exception as exc:
                if not events:
                    log.warning("Public-search discovery failed: %s", exc)
                break
            batch = (data.get("events") or []) if isinstance(data, dict) else []
            for event in batch:
                if isinstance(event, dict) and is_temperature_event(event):
                    events[str(event.get("id") or event.get("slug"))] = event
            pagination = (data.get("pagination") or {}) if isinstance(data, dict) else {}
            if not pagination.get("hasMore"):
                break
            page += 1

        new_tokens: list[str] = []
        seen_market_ids: set[str] = set()
        registered = 0
        for event in events.values():
            event_id = str(event.get("id") or "")
            event_slug = str(event.get("slug") or "")
            event_title = str(event.get("title") or "")
            event_end = event.get("endDate") or event.get("eventDate")
            for market in event.get("markets") or []:
                if not isinstance(market, dict):
                    continue
                if market.get("closed") is True or market.get("active") is False:
                    continue
                if market.get("enableOrderBook") is False or market.get("acceptingOrders") is False:
                    continue
                market_end = market.get("endDate") or event_end
                yes_token, no_token = yes_token_from_market(market)
                if not yes_token:
                    continue
                market_id = str(market.get("id") or "")
                if not market_id:
                    continue
                enabled, fee_rate = extract_fee_info(market)
                m = MarketInfo(
                    market_id=market_id,
                    event_id=event_id,
                    condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
                    event_slug=event_slug,
                    event_title=event_title,
                    market_slug=str(market.get("slug") or ""),
                    question=str(market.get("question") or ""),
                    temperature_label=market_temperature_label(market),
                    end_date=str(market_end or "") or None,
                    yes_token_id=yes_token,
                    no_token_id=no_token,
                    fees_enabled=enabled,
                    fee_rate=fee_rate,
                )
                seen_market_ids.add(market_id)
                was_new = self.register_market_memory(m)
                self.db.upsert_market(m)
                registered += 1
                if was_new:
                    new_tokens.append(yes_token)

        if events and seen_market_ids:
            self.db.deactivate_missing_markets(seen_market_ids)
            open_ids = set(self.db.open_market_ids())
            for token, cached in list(self.markets_by_yes.items()):
                if cached.market_id not in seen_market_ids and cached.market_id not in open_ids:
                    self.markets_by_yes.pop(token, None)
                    self.markets_by_id.pop(cached.market_id, None)
                    if cached.condition_id:
                        self.markets_by_condition.pop(cached.condition_id.lower(), None)
                    self.tops.pop(token, None)
                    self.books.pop(token, None)
                    self.last_asks.pop(token, None)

        self.last_discovery_at = iso_now()
        self.db.set_meta("last_discovery_at", self.last_discovery_at)
        self.db.set_meta("last_discovery_event_count", str(len(events)))
        if new_tokens and self.ws_connected:
            for token in new_tokens:
                await self.dynamic_subscribe_queue.put(token)
        ev_count, market_count = self.db.active_counts()
        log.info("Discovery: events=%d tracked_yes_markets=%d new_tokens=%d", ev_count, market_count, len(new_tokens))
        return registered

    async def discovery_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(DISCOVERY_INTERVAL)
                await self.discover_markets()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("Discovery loop error: %s", exc)
                await asyncio.sleep(10)

    async def websocket_loop(self) -> None:
        assert self.session is not None
        backoff = 2
        while True:
            try:
                tokens = list(self.markets_by_yes.keys())
                if not tokens:
                    await asyncio.sleep(10)
                    continue
                log.info("Connecting CLOB WebSocket; YES assets=%d", len(tokens))
                # A reconnect is a data gap. Do not compare the first fresh quote to a
                # possibly hours-old pre-disconnect ask and call that a threshold crossing.
                for token in tokens:
                    self.last_asks.pop(token, None)
                async with self.session.ws_connect(CLOB_WS, heartbeat=None, receive_timeout=40, max_msg_size=4 * 1024 * 1024) as ws:
                    self.ws = ws
                    self.ws_connected = True
                    backoff = 2
                    first = tokens[:500]
                    await ws.send_json({
                        "assets_ids": first,
                        "type": "market",
                        "custom_feature_enabled": True,
                    })
                    for i in range(500, len(tokens), 500):
                        await ws.send_json({
                            "operation": "subscribe",
                            "assets_ids": tokens[i:i + 500],
                            "custom_feature_enabled": True,
                        })
                    ping_task = asyncio.create_task(self.ws_ping_loop(ws))
                    dynamic_task = asyncio.create_task(self.ws_dynamic_subscribe_loop(ws))
                    try:
                        async for msg in ws:
                            if msg.type == WSMsgType.TEXT:
                                if msg.data == "PONG":
                                    continue
                                self.last_ws_message_at = iso_now()
                                try:
                                    data = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    continue
                                if isinstance(data, list):
                                    for item in data:
                                        if isinstance(item, dict):
                                            await self.handle_ws_event(item)
                                elif isinstance(data, dict):
                                    await self.handle_ws_event(data)
                            elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                                break
                    finally:
                        ping_task.cancel()
                        dynamic_task.cancel()
                        await asyncio.gather(ping_task, dynamic_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("WebSocket disconnected: %s; reconnecting", exc)
            finally:
                self.ws_connected = False
                self.ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def ws_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            await asyncio.sleep(10)
            await ws.send_str("PING")

    async def ws_dynamic_subscribe_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            token = await self.dynamic_subscribe_queue.get()
            batch = [token]
            while len(batch) < 250:
                try:
                    batch.append(self.dynamic_subscribe_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await ws.send_json({
                "operation": "subscribe",
                "assets_ids": batch,
                "custom_feature_enabled": True,
            })
            log.info("WebSocket subscribed to %d newly discovered YES assets", len(batch))

    async def handle_ws_event(self, data: dict[str, Any]) -> None:
        etype = str(data.get("event_type") or data.get("type") or "")
        if etype == "book":
            token = str(data.get("asset_id") or "")
            if token not in self.markets_by_yes:
                return
            bids = self.level_dict(data.get("bids") or [])
            asks = self.level_dict(data.get("asks") or [])
            self.books[token] = {"bids": bids, "asks": asks}
            bid = max(bids) if bids else None
            ask = min(asks) if asks else None
            await self.handle_top(token, bid, ask, data.get("timestamp"))
            return

        if etype == "price_change":
            for change in data.get("price_changes") or []:
                if not isinstance(change, dict):
                    continue
                token = str(change.get("asset_id") or "")
                if token not in self.markets_by_yes:
                    continue
                price = as_float(change.get("price"))
                size = as_float(change.get("size"), 0.0) or 0.0
                side = str(change.get("side") or "").upper()
                if price is not None:
                    book = self.books.setdefault(token, {"bids": {}, "asks": {}})
                    side_book = book["bids"] if side == "BUY" else book["asks"]
                    if size <= 0:
                        side_book.pop(price, None)
                    else:
                        side_book[price] = size
                bid = as_float(change.get("best_bid"))
                ask = as_float(change.get("best_ask"))
                if bid is None:
                    bids = self.books.get(token, {}).get("bids", {})
                    bid = max(bids) if bids else None
                if ask is None:
                    asks = self.books.get(token, {}).get("asks", {})
                    ask = min(asks) if asks else None
                await self.handle_top(token, bid, ask, data.get("timestamp"))
            return

        if etype == "best_bid_ask":
            token = str(data.get("asset_id") or "")
            if token in self.markets_by_yes:
                await self.handle_top(
                    token,
                    as_float(data.get("best_bid")),
                    as_float(data.get("best_ask")),
                    data.get("timestamp"),
                )
            return

        if etype == "market_resolved":
            condition = str(data.get("market") or "").lower()
            m = self.markets_by_condition.get(condition)
            if not m:
                return
            winning_asset = str(data.get("winning_asset_id") or "")
            winning_outcome = str(data.get("winning_outcome") or "")
            if winning_asset:
                yes_value = 1.0 if winning_asset == m.yes_token_id else 0.0
                await self.settle_market(m, yes_value, winning_outcome or ("Yes" if yes_value == 1 else "No"), "websocket")

    @staticmethod
    def level_dict(levels: Iterable[dict[str, Any]]) -> dict[float, float]:
        result: dict[float, float] = {}
        for lvl in levels:
            if not isinstance(lvl, dict):
                continue
            p = as_float(lvl.get("price"))
            s = as_float(lvl.get("size"))
            if p is not None and s is not None and 0 < p <= 1 and s > 0:
                result[p] = s
        return result

    async def handle_top(self, token: str, bid: float | None, ask: float | None, timestamp: Any) -> None:
        top = self.tops.setdefault(token, TopOfBook())
        if bid is not None and 0 <= bid <= 1:
            top.best_bid = bid
        if ask is not None and 0 < ask <= 1:
            top.best_ask = ask
        try:
            top.timestamp_ms = int(timestamp) if timestamp is not None else int(time.time() * 1000)
        except (TypeError, ValueError):
            top.timestamp_ms = int(time.time() * 1000)

        m = self.markets_by_yes.get(token)
        if not m:
            return

        # A resolved market can continue emitting terminal CLOB prices briefly.
        # endDate is intentionally NOT used here: for weather events it is a calendar
        # marker and is not always the exact moment order acceptance stops.
        market_ended = m.market_id in self.closed_market_ids

        # Risk management remains active while entries are paused, but never after
        # an explicit resolution; after that we only wait for/record settlement.
        if (
            not market_ended
            and STOP_LOSS_ENABLED
            and bid is not None
            and 0 < bid <= STOP_LOSS_PRICE
        ):
            self.maybe_trigger_stop_losses(token, bid)

        if ask is None or not (0 < ask <= 1):
            return
        previous = self.last_asks.get(token)
        self.last_asks[token] = ask
        if previous is None:
            # Startup/new-market/reconnect arming: first quote is baseline only.
            return
        if market_ended:
            return
        # Exact $1 asks are common around settlement. Never treat a boundary quote
        # as a fresh 0.79/0.84/0.89 crossing even if an upstream status is stale.
        if ask >= 1.0 - 1e-12:
            return
        if not self.trading_enabled:
            # Keep tracking prices while paused, but do not consume/record entry signals.
            return
        crossed: list[tuple[float, str, int]] = []
        for threshold in THRESHOLDS:
            strategy = strategy_name(threshold)
            key = (strategy, m.market_id)
            if key in self.signal_keys:
                continue
            if previous < threshold <= ask:
                signal_id = self.db.insert_signal(strategy, threshold, m.market_id, previous, ask)
                self.signal_keys.add(key)
                if signal_id is not None:
                    crossed.append((threshold, strategy, signal_id))
        if crossed and m.market_id not in self.pending_crossings:
            self.pending_crossings.add(m.market_id)
            asyncio.create_task(self.process_crossings(m, previous, ask, crossed))
        elif crossed:
            # Extremely rare concurrent top event; process independently to avoid losing signals.
            asyncio.create_task(self.process_crossings(m, previous, ask, crossed))

    async def fetch_book_levels(self, token: str) -> list[dict[str, float]]:
        try:
            data = await self.clob_get("/book", {"token_id": token})
            asks = data.get("asks") or [] if isinstance(data, dict) else []
            levels = []
            for x in asks:
                p = as_float(x.get("price")) if isinstance(x, dict) else None
                s = as_float(x.get("size")) if isinstance(x, dict) else None
                if p is not None and s is not None and 0 < p <= 1 and s > 0:
                    levels.append({"price": p, "size": s})
            if levels:
                return sorted(levels, key=lambda x: x["price"])
        except Exception as exc:
            log.warning("REST orderbook fetch failed token=%s: %s", token[-8:], exc)
        cached = self.books.get(token, {}).get("asks", {})
        return [{"price": p, "size": s} for p, s in sorted(cached.items())]

    async def fetch_bid_levels(self, token: str) -> list[dict[str, float]]:
        try:
            data = await self.clob_get("/book", {"token_id": token})
            bids = data.get("bids") or [] if isinstance(data, dict) else []
            levels = []
            for x in bids:
                p = as_float(x.get("price")) if isinstance(x, dict) else None
                s = as_float(x.get("size")) if isinstance(x, dict) else None
                if p is not None and s is not None and 0 < p <= 1 and s > 0:
                    levels.append({"price": p, "size": s})
            if levels:
                return sorted(levels, key=lambda x: x["price"], reverse=True)
        except Exception as exc:
            log.warning("REST bid book fetch failed token=%s: %s", token[-8:], exc)
        cached = self.books.get(token, {}).get("bids", {})
        return [{"price": p, "size": s} for p, s in sorted(cached.items(), reverse=True)]

    @staticmethod
    def simulate_market_sell(bids: list[dict[str, float]], target_shares: float) -> SellFillResult:
        remaining = target_shares
        sold = 0.0
        proceeds = 0.0
        levels_used: list[dict[str, float]] = []
        for lvl in sorted(bids, key=lambda x: x["price"], reverse=True):
            p = float(lvl["price"])
            available_shares = float(lvl["size"])
            if p <= 0 or p > 1 or available_shares <= 0 or remaining <= 1e-10:
                continue
            qty = min(remaining, available_shares)
            notional = qty * p
            sold += qty
            proceeds += notional
            remaining -= qty
            levels_used.append({"price": p, "shares": qty, "notional": notional})
        avg = proceeds / sold if sold > 0 else None
        return SellFillResult(target_shares, sold, proceeds, avg, levels_used)

    def maybe_trigger_stop_losses(self, token: str, trigger_bid: float) -> None:
        for tr in self.db.open_trades_by_token(token):
            trade_id = int(tr["id"])
            if trade_id in self.pending_stop_losses:
                continue
            self.pending_stop_losses.add(trade_id)
            asyncio.create_task(self.process_stop_loss(trade_id, token, trigger_bid))

    async def process_stop_loss(self, trade_id: int, token: str, trigger_bid: float) -> None:
        try:
            current_rows = [r for r in self.db.open_trades_by_token(token) if int(r["id"]) == trade_id]
            if not current_rows:
                return
            tr = current_rows[0]
            bids = await self.fetch_bid_levels(token)
            sell = self.simulate_market_sell(bids, float(tr["shares"]))
            if sell.sold_shares <= 0 or sell.avg_price is None:
                log.warning("STOP LOSS no bid liquidity trade=%s token=%s", trade_id, token[-8:])
                return
            # A stop is modeled as a market/FOK exit; do not pretend unsold shares disappeared.
            if sell.full_fill_ratio < MIN_FULL_FILL_RATIO:
                log.warning(
                    "STOP LOSS insufficient bid liquidity trade=%s fill_ratio=%.4f",
                    trade_id, sell.full_fill_ratio,
                )
                return
            m = self.markets_by_yes.get(token)
            if not m:
                return
            if m.market_id in self.closed_market_ids:
                return
            if not await self.market_accepting_orders(m):
                # Do not manufacture a stop from settlement/closed-book quotes.
                return
            fee_rate = await self.get_fee_rate(m)
            fee_fill = FillResult(
                requested_notional=sell.gross_proceeds,
                filled_notional=sell.gross_proceeds,
                shares=sell.sold_shares,
                avg_price=sell.avg_price,
                levels=sell.levels,
            )
            exit_fee = self.taker_fee(fee_fill, fee_rate)
            if m.market_id in self.closed_market_ids:
                return
            closed = self.db.close_stop_loss(trade_id, trigger_bid, sell, exit_fee)
            if not closed:
                return
            msg = (
                f"🛑 PAPER STOP-LOSS {closed['strategy']}\n"
                f"{closed['event_title']}\n"
                f"Outcome: {closed['temperature_label']}\n"
                f"Trigger bid: ${trigger_bid:.3f} ≤ ${STOP_LOSS_PRICE:.2f}\n"
                f"Exit avg: ${float(closed['exit_price']):.4f}\n"
                f"Gross: ${float(closed['exit_gross_proceeds']):.4f}\n"
                f"Exit fee: ${float(closed['exit_fee']):.5f}\n"
                f"Net returned: ${float(closed['exit_net_proceeds']):.4f}\n"
                f"PnL: {fmt_money(float(closed['pnl']))}"
            )
            log.info(msg.replace("\n", " | "))
            if self.telegram:
                await self.telegram.send_message(msg)
        except Exception as exc:
            log.exception("Stop-loss processing failed trade=%s: %s", trade_id, exc)
        finally:
            self.pending_stop_losses.discard(trade_id)

    @staticmethod
    def entry_liquidity_window(
        asks: list[dict[str, float]],
        max_slippage: float,
    ) -> tuple[list[dict[str, float]], float | None, float | None, float]:
        """Return asks close enough to the current best ask and their USD notional depth.

        For paper execution we deliberately ignore far-away asks. A thin book with
        $0.50 at 0.79 and the rest at 0.95 should not be treated as a realistic $5
        fill near the signal.
        """
        valid: list[dict[str, float]] = []
        for lvl in asks:
            try:
                p = float(lvl["price"])
                size = float(lvl["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if 0 < p <= 1 and size > 0:
                valid.append({"price": p, "size": size})
        if not valid:
            return [], None, None, 0.0
        valid.sort(key=lambda x: x["price"])
        best_ask = valid[0]["price"]
        max_price = min(1.0, best_ask + max(0.0, max_slippage))
        eligible = [lvl for lvl in valid if lvl["price"] <= max_price + 1e-12]
        notional = sum(lvl["price"] * lvl["size"] for lvl in eligible)
        return eligible, best_ask, max_price, notional

    @staticmethod
    def simulate_market_buy(asks: list[dict[str, float]], target_notional: float) -> FillResult:
        remaining = target_notional
        shares = 0.0
        filled = 0.0
        levels_used: list[dict[str, float]] = []
        for lvl in sorted(asks, key=lambda x: x["price"]):
            p = float(lvl["price"])
            available_shares = float(lvl["size"])
            if p <= 0 or p > 1 or available_shares <= 0 or remaining <= 1e-10:
                continue
            max_notional = p * available_shares
            use_notional = min(remaining, max_notional)
            qty = use_notional / p
            shares += qty
            filled += use_notional
            remaining -= use_notional
            levels_used.append({"price": p, "shares": qty, "notional": use_notional})
        avg = filled / shares if shares > 0 else None
        return FillResult(target_notional, filled, shares, avg, levels_used)

    async def get_fee_rate(self, m: MarketInfo) -> float:
        if m.market_id in self.fee_rate_cache:
            return self.fee_rate_cache[m.market_id]
        if m.fee_rate is not None:
            rate = max(0.0, float(m.fee_rate))
            self.fee_rate_cache[m.market_id] = rate
            return rate
        if m.fees_enabled is False:
            self.fee_rate_cache[m.market_id] = 0.0
            return 0.0
        if m.condition_id:
            try:
                info = await self.clob_get(f"/clob-markets/{m.condition_id}")
                fd = info.get("fd") or {} if isinstance(info, dict) else {}
                rate = as_float(fd.get("r")) if isinstance(fd, dict) else None
                if rate is not None:
                    rate = max(0.0, rate)
                    self.fee_rate_cache[m.market_id] = rate
                    return rate
            except Exception as exc:
                log.debug("Fee lookup failed market=%s: %s", m.market_id, exc)
        # Current Weather category fallback; report still records the fee explicitly.
        rate = WEATHER_FEE_RATE_FALLBACK
        self.fee_rate_cache[m.market_id] = rate
        return rate

    @staticmethod
    def taker_fee(fill: FillResult, fee_rate: float) -> float:
        fee = 0.0
        for lvl in fill.levels:
            p = float(lvl["price"])
            c = float(lvl["shares"])
            fee += c * fee_rate * p * (1.0 - p)
        return round(fee + 1e-12, 5)

    async def process_crossings(
        self,
        m: MarketInfo,
        previous_ask: float,
        trigger_ask: float,
        crossed: list[tuple[float, str, int]],
    ) -> None:
        try:
            # Re-check immediately before simulated execution: resolution/status can
            # change between the websocket signal and the REST order-book fetch.
            if m.market_id in self.closed_market_ids:
                for _threshold, _strategy, signal_id in crossed:
                    self.db.update_signal(signal_id, "SKIPPED", "MARKET_RESOLVED_BEFORE_FILL")
                return
            if not await self.market_accepting_orders(m):
                for _threshold, _strategy, signal_id in crossed:
                    self.db.update_signal(signal_id, "SKIPPED", "MARKET_NOT_ACCEPTING_ORDERS")
                return
            asks = await self.fetch_book_levels(m.yes_token_id)
            eligible_asks, rest_best_ask, max_entry_price, near_ask_liquidity_usd = self.entry_liquidity_window(
                asks, MAX_ENTRY_SLIPPAGE
            )
            if ENTRY_LIQUIDITY_CHECK_ENABLED:
                required_liquidity = max(MIN_ENTRY_LIQUIDITY_USD, TRADE_NOTIONAL_USD if REQUIRE_FULL_FILL else 0.0)
                if rest_best_ask is None or not eligible_asks:
                    for _threshold, _strategy, signal_id in crossed:
                        self.db.update_signal(signal_id, "SKIPPED", "NO_ASK_LIQUIDITY")
                    return
                if near_ask_liquidity_usd + 1e-9 < required_liquidity:
                    reason = (
                        f"INSUFFICIENT_NEAR_ASK_LIQUIDITY available_usd={near_ask_liquidity_usd:.4f} "
                        f"required_usd={required_liquidity:.4f} best_ask={rest_best_ask:.4f} "
                        f"max_price={max_entry_price:.4f}"
                    )
                    for _threshold, _strategy, signal_id in crossed:
                        self.db.update_signal(signal_id, "SKIPPED", reason)
                    log.info(
                        "SKIP thin ask book market=%s available=$%.4f required=$%.4f range=%.4f..%.4f",
                        m.market_id, near_ask_liquidity_usd, required_liquidity, rest_best_ask, max_entry_price,
                    )
                    return
            execution_asks = eligible_asks if ENTRY_LIQUIDITY_CHECK_ENABLED else asks
            fill = self.simulate_market_buy(execution_asks, TRADE_NOTIONAL_USD)
            fee_rate = await self.get_fee_rate(m)
            fee = self.taker_fee(fill, fee_rate)
            top = self.tops.get(m.yes_token_id, TopOfBook())

            if m.market_id in self.closed_market_ids:
                for _threshold, _strategy, signal_id in crossed:
                    self.db.update_signal(signal_id, "SKIPPED", "MARKET_RESOLVED_DURING_FILL")
                return

            for threshold, strategy, signal_id in crossed:
                if fill.shares <= 0 or fill.avg_price is None:
                    self.db.update_signal(signal_id, "SKIPPED", "NO_ASK_LIQUIDITY")
                    continue
                if REQUIRE_FULL_FILL and fill.full_fill_ratio < MIN_FULL_FILL_RATIO:
                    self.db.update_signal(
                        signal_id,
                        "SKIPPED",
                        f"INSUFFICIENT_LIQUIDITY fill_ratio={fill.full_fill_ratio:.4f}",
                    )
                    continue
                cash_debit = fill.filled_notional + fee
                cash = self.db.free_cash(strategy)
                if cash + 1e-9 < cash_debit:
                    self.db.update_signal(signal_id, "SKIPPED", f"INSUFFICIENT_PAPER_CASH cash={cash:.4f}")
                    continue

                values = {
                    "strategy": strategy,
                    "threshold": threshold,
                    "market_id": m.market_id,
                    "event_id": m.event_id,
                    "event_title": m.event_title,
                    "event_slug": m.event_slug,
                    "temperature_label": m.temperature_label,
                    "yes_token_id": m.yes_token_id,
                    "signal_time": iso_now(),
                    "fill_time": iso_now(),
                    "previous_ask": previous_ask,
                    "trigger_ask": trigger_ask,
                    "requested_notional": TRADE_NOTIONAL_USD,
                    "filled_notional": fill.filled_notional,
                    "taker_fee": fee,
                    "cash_debit": cash_debit,
                    "shares": fill.shares,
                    "avg_fill_price": fill.avg_price,
                    "best_bid_at_fill": top.best_bid,
                    "best_ask_at_fill": top.best_ask,
                    "fill_levels_json": json.dumps(fill.levels, separators=(",", ":")),
                }
                try:
                    trade_id = self.db.insert_trade(values)
                except sqlite3.IntegrityError:
                    self.db.update_signal(signal_id, "SKIPPED", "DUPLICATE_TRADE")
                    continue
                self.db.update_signal(signal_id, "BOUGHT", None, trade_id)
                msg = (
                    f"🧪 PAPER BUY {strategy}\n"
                    f"{m.event_title}\n"
                    f"Outcome: {m.temperature_label}\n"
                    f"Threshold: {threshold:.2f} | crossed {previous_ask:.3f} → {trigger_ask:.3f}\n"
                    f"Fill avg: {fill.avg_price:.4f}\n"
                    f"Shares: {fill.shares:.4f}\n"
                    f"Notional: ${fill.filled_notional:.2f}\n"
                    f"Fee: ${fee:.5f}\n"
                    f"Cash used: ${cash_debit:.5f}"
                )
                log.info(msg.replace("\n", " | "))
                if self.telegram:
                    await self.telegram.send_message(msg)
                if STOP_LOSS_ENABLED and top.best_bid is not None and 0 < top.best_bid <= STOP_LOSS_PRICE:
                    self.maybe_trigger_stop_losses(m.yes_token_id, top.best_bid)
        except Exception as exc:
            log.exception("Crossing processing failed market=%s: %s", m.market_id, exc)
            for _, _, signal_id in crossed:
                self.db.update_signal(signal_id, "ERROR", str(exc)[:300])
        finally:
            self.pending_crossings.discard(m.market_id)

    async def resolve_open_positions_once(self) -> None:
        ids = self.db.open_market_ids()
        if not ids:
            self.last_resolution_scan_at = iso_now()
            return
        sem = asyncio.Semaphore(12)

        async def check_one(market_id: str) -> None:
            async with sem:
                try:
                    market = await self.gamma_get(f"/markets/{market_id}")
                    if not isinstance(market, dict) or market.get("closed") is not True:
                        return
                    outcomes = parse_jsonish(market.get("outcomes"), []) or []
                    prices = parse_jsonish(market.get("outcomePrices"), []) or []
                    if len(outcomes) != len(prices) or not outcomes:
                        return
                    yes_value: float | None = None
                    for i, outcome in enumerate(outcomes):
                        if str(outcome).strip().lower() == "yes":
                            yes_value = as_float(prices[i])
                            break
                    if yes_value is None:
                        return
                    status = str(market.get("umaResolutionStatus") or "").lower()
                    boundary_value = (
                        abs(yes_value - 0.0) < 1e-9
                        or abs(yes_value - 1.0) < 1e-9
                        or abs(yes_value - 0.5) < 1e-9
                    )
                    if not boundary_value and "resolved" not in status:
                        return
                    yes_value = min(1.0, max(0.0, yes_value))
                    m = self.markets_by_id.get(market_id)
                    if not m:
                        row = self.db.market_row(market_id)
                        if not row:
                            return
                        m = MarketInfo(
                            market_id=market_id,
                            event_id=str(row["event_id"]),
                            condition_id=str(row["condition_id"] or ""),
                            event_slug=str(row["event_slug"] or ""),
                            event_title=str(row["event_title"] or ""),
                            market_slug=str(row["market_slug"] or ""),
                            question=str(row["question"] or ""),
                            temperature_label=str(row["temperature_label"] or ""),
                            end_date=row["end_date"],
                            yes_token_id=str(row["yes_token_id"]),
                            no_token_id=str(row["no_token_id"]) if row["no_token_id"] else None,
                        )
                    outcome = "Yes" if yes_value == 1 else "No" if yes_value == 0 else f"Yes value {yes_value:g}"
                    await self.settle_market(m, yes_value, outcome, "gamma_poll")
                except Exception as exc:
                    log.debug("Resolution check failed market=%s: %s", market_id, exc)

        await asyncio.gather(*(check_one(mid) for mid in ids))
        self.last_resolution_scan_at = iso_now()

    async def stop_loss_scan_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(STOP_LOSS_SCAN_INTERVAL)
                if not STOP_LOSS_ENABLED:
                    continue
                for tr in self.db.open_trades():
                    token = str(tr["yes_token_id"])
                    m = self.markets_by_yes.get(token)
                    if not m or m.market_id in self.closed_market_ids:
                        continue
                    top = self.tops.get(token)
                    if top and top.best_bid is not None and 0 < top.best_bid <= STOP_LOSS_PRICE:
                        self.maybe_trigger_stop_losses(token, top.best_bid)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Stop-loss scan error: %s", exc)

    async def resolution_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(RESOLUTION_INTERVAL)
                await self.resolve_open_positions_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("Resolution loop error: %s", exc)
                await asyncio.sleep(10)

    async def settle_market(self, m: MarketInfo, yes_value: float, outcome: str, source: str) -> None:
        resolved_at = iso_now()
        self.closed_market_ids.add(m.market_id)
        self.db.mark_market_resolved(m.market_id, yes_value, resolved_at)
        settled = self.db.settle_trades(m.market_id, yes_value, outcome, source, resolved_at)
        if not settled:
            return
        lines = [
            "✅ PAPER REDEEM",
            m.event_title,
            f"Outcome: {m.temperature_label}",
            f"Resolution: {outcome} (YES={yes_value:g})",
        ]
        for tr in settled:
            lines.append(
                f"{tr['strategy']}: payout ${tr['payout']:.4f} | PnL {fmt_money(float(tr['pnl']))}"
            )
        msg = "\n".join(lines)
        log.info(msg.replace("\n", " | "))
        if self.telegram:
            await self.telegram.send_message(msg)

    async def price_snapshot_loop(self) -> None:
        while True:
            await asyncio.sleep(PRICE_SNAPSHOT_INTERVAL)
            try:
                for token, top in self.tops.items():
                    m = self.markets_by_yes.get(token)
                    if m:
                        self.db.save_top_price(m.market_id, token, top)
                self.db.flush()
            except Exception as exc:
                log.warning("Price snapshot error: %s", exc)

    async def equity_snapshot_loop(self) -> None:
        while True:
            try:
                for threshold in THRESHOLDS:
                    strategy = strategy_name(threshold)
                    stat = self.db.stats(strategy, self.tops)
                    self.db.snapshot_equity(strategy, stat)
                await asyncio.sleep(EQUITY_SNAPSHOT_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Equity snapshot error: %s", exc)
                await asyncio.sleep(10)

    def all_stats(self) -> list[dict[str, Any]]:
        return [self.db.stats(strategy_name(t), self.tops) for t in THRESHOLDS]

    def status_text(self) -> str:
        events, markets = self.db.active_counts()
        lines = [
            "🌡 WEATHER PAPER STATUS",
            f"Active city/date events: {events}",
            f"Tracked YES markets: {markets}",
            f"Paper entries: {'▶️ RUNNING' if self.trading_enabled else '⏹ STOPPED'}",
            f"Stop-loss: {'ON @ $' + format(STOP_LOSS_PRICE, '.2f') if STOP_LOSS_ENABLED else 'OFF'}",
            f"WebSocket: {'CONNECTED' if self.ws_connected else 'DISCONNECTED'}",
            f"Last discovery: {self.last_discovery_at or '—'}",
            "",
        ]
        for s in self.all_stats():
            lines.extend([
                f"{s['strategy']} ({s['threshold']:.2f})",
                f"Equity ${s['equity']:.2f} | cash ${s['cash']:.2f}",
                f"PnL {fmt_money(s['total_pnl'])} | ROI {s['roi_pct']:+.2f}%",
                f"Trades {s['trades']} | open {s['open_count']} | SL {s['stop_loss_count']} | W/L {s['wins']}/{s['losses']} | WR {s['win_rate']:.1f}%",
                f"Fees ${s['fees']:.4f} | Max DD {s['max_drawdown_pct']:.2f}%",
                "",
            ])
        return "\n".join(lines).rstrip()

    def open_text(self) -> str:
        rows = self.db.open_trades()
        if not rows:
            return "No open paper positions."
        lines = [f"OPEN PAPER POSITIONS: {len(rows)}"]
        for tr in rows[-25:]:
            top = self.tops.get(str(tr["yes_token_id"]))
            bid = top.best_bid if top else None
            mtm = float(tr["shares"]) * bid - float(tr["cash_debit"]) if bid is not None else None
            lines.append(
                f"{tr['strategy']} | {tr['temperature_label']} | avg {tr['avg_fill_price']:.3f} | "
                f"bid {bid:.3f} | uPnL {fmt_money(mtm)} | {tr['event_title']}"
                if bid is not None
                else f"{tr['strategy']} | {tr['temperature_label']} | avg {tr['avg_fill_price']:.3f} | {tr['event_title']}"
            )
        return "\n".join(lines)[:4096]

    def latest_text(self) -> str:
        rows = self.db.latest_trades(20)
        if not rows:
            return "No paper trades yet."
        lines = ["LATEST PAPER TRADES"]
        for tr in rows:
            pnl = fmt_money(float(tr["pnl"])) if tr["pnl"] is not None else "OPEN"
            lines.append(
                f"#{tr['id']} {tr['strategy']} {tr['temperature_label']} @ {tr['avg_fill_price']:.3f} | {pnl} | {tr['event_title']}"
            )
        return "\n".join(lines)[:4096]

    async def telegram_loop(self) -> None:
        if not self.telegram or not self.telegram.enabled:
            return
        help_text = (
            "Кнопки управления paper-ботом:\n"
            "▶️ Старт — разрешить новые виртуальные входы\n"
            "⏹ Стоп — запретить новые входы; стоп-лосс и resolution продолжают работать\n"
            "📂 Позиции — открытые позиции\n"
            "📊 Отчёт — текущая статистика + ZIP\n\n"
            "/status — balances and PnL\n"
            "/last — latest trades\n"
            "/markets — current market counts\n"
            "/help — show this menu"
        )
        while True:
            updates = await self.telegram.get_updates()
            for update in updates:
                callback = update.get("callback_query") or {}
                if callback:
                    callback_id = str(callback.get("id") or "")
                    data = str(callback.get("data") or "")
                    cb_msg = callback.get("message") or {}
                    chat_id = cb_msg.get("chat", {}).get("id")
                    if chat_id is None:
                        await self.telegram.answer_callback(callback_id)
                        continue
                    if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
                        await self.telegram.answer_callback(callback_id, "Not authorized")
                        continue
                    if data == "paper_start":
                        self.set_trading_enabled(True)
                        await self.telegram.answer_callback(callback_id, "Paper trading started")
                        await self.telegram.send_message(
                            "▶️ Новые PAPER-входы ВКЛЮЧЕНЫ.\nСтоп-лосс и resolution активны.",
                            chat_id, self.telegram_keyboard(),
                        )
                    elif data == "paper_stop":
                        self.set_trading_enabled(False)
                        await self.telegram.answer_callback(callback_id, "New entries stopped")
                        await self.telegram.send_message(
                            "⏹ Новые PAPER-входы ОСТАНОВЛЕНЫ.\nОткрытые позиции НЕ закрываются: stop-loss $%.2f и resolution продолжают работать." % STOP_LOSS_PRICE,
                            chat_id, self.telegram_keyboard(),
                        )
                    elif data == "paper_positions":
                        await self.telegram.answer_callback(callback_id)
                        await self.telegram.send_message(self.open_text(), chat_id, self.telegram_keyboard())
                    elif data == "paper_report":
                        await self.telegram.answer_callback(callback_id, "Building report")
                        await self.telegram.send_message(self.status_text(), chat_id, self.telegram_keyboard())
                        path = self.build_report_zip()
                        await self.telegram.send_document(path, "Weather paper-trading report", chat_id)
                    else:
                        await self.telegram.answer_callback(callback_id)
                    continue

                msg = update.get("message") or {}
                text = str(msg.get("text") or "").strip()
                chat_id = msg.get("chat", {}).get("id")
                if not text or chat_id is None:
                    continue
                if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
                    continue
                command = text.split()[0].split("@")[0].lower()
                if command in {"/start", "/help"}:
                    await self.telegram.send_message(help_text, chat_id, self.telegram_keyboard())
                elif command in {"/status", "/stats"}:
                    await self.telegram.send_message(self.status_text(), chat_id, self.telegram_keyboard())
                elif command == "/open":
                    await self.telegram.send_message(self.open_text(), chat_id, self.telegram_keyboard())
                elif command == "/last":
                    await self.telegram.send_message(self.latest_text(), chat_id, self.telegram_keyboard())
                elif command == "/markets":
                    events, markets = self.db.active_counts()
                    await self.telegram.send_message(
                        f"Tracked active city/date events: {events}\nTracked active YES markets: {markets}\nAll markets ever seen: {self.db.total_market_count()}",
                        chat_id, self.telegram_keyboard(),
                    )
                elif command == "/report":
                    await self.telegram.send_message(self.status_text(), chat_id, self.telegram_keyboard())
                    path = self.build_report_zip()
                    await self.telegram.send_document(path, "Weather paper-trading report", chat_id)
            await asyncio.sleep(0.2)

    def build_report_zip(self) -> str:
        Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)
        stamp = utc_now().strftime("%Y-%m-%d_%H-%M-%S_UTC")
        zip_path = str(Path(REPORT_DIR) / f"weather_paper_report_{stamp}.zip")

        def csv_bytes(headers: list[str], rows: Iterable[Iterable[Any]]) -> bytes:
            sio = io.StringIO(newline="")
            writer = csv.writer(sio)
            writer.writerow(headers)
            writer.writerows(rows)
            return sio.getvalue().encode("utf-8-sig")

        stats = self.all_stats()
        summary_headers = list(stats[0].keys()) if stats else []
        summary_rows = [[s.get(h) for h in summary_headers] for s in stats]

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("summary.csv", csv_bytes(summary_headers, summary_rows))
            for table in ("trades", "signals", "markets", "top_prices", "equity_snapshots"):
                headers, rows = self.db.export_rows(table)
                zf.writestr(f"{table}.csv", csv_bytes(headers, ([r[h] for h in headers] for r in rows)))
            zf.writestr(
                "about.txt",
                (
                    "Polymarket Highest Temperature PAPER trading report\n"
                    f"Generated: {iso_now()}\n"
                    f"Thresholds: {THRESHOLDS}\n"
                    f"Requested notional per signal: ${TRADE_NOTIONAL_USD}\n"
                    f"Starting balance per strategy: ${START_BALANCE_USD}\n"
                    f"New paper entries: {'RUNNING' if self.trading_enabled else 'STOPPED'}\n"
                    f"Stop-loss: {'enabled at best bid <= $' + format(STOP_LOSS_PRICE, '.2f') if STOP_LOSS_ENABLED else 'disabled'}\n"
                    "Execution: real public CLOB depth, paper only.\n"
                    "PnL includes simulated taker fees.\n"
                    "No real orders, wallet, signing, private key, or on-chain redeem.\n"
                ).encode("utf-8"),
            )
        self.cleanup_reports()
        return zip_path

    def cleanup_reports(self) -> None:
        cutoff = time.time() - 7 * 86400
        for p in Path(REPORT_DIR).glob("weather_paper_report_*.zip"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
            except OSError:
                pass

    async def hourly_report_loop(self) -> None:
        while True:
            now = utc_now()
            next_hour = (now.replace(minute=0, second=5, microsecond=0) + timedelta(hours=1))
            await asyncio.sleep(max(1.0, (next_hour - now).total_seconds()))
            try:
                if self.telegram and self.telegram.default_chat_id:
                    await self.telegram.send_message(self.status_text())
                    path = self.build_report_zip()
                    await self.telegram.send_document(path, f"Hourly report {utc_now():%Y-%m-%d %H:%M} UTC")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Hourly report error: %s", exc)

    # ------------------------------------------------------------------
    # Web dashboard / health server
    # ------------------------------------------------------------------

    def dashboard_allowed(self, request: web.Request) -> bool:
        if not DASHBOARD_TOKEN:
            return True
        return request.query.get("token") == DASHBOARD_TOKEN or request.headers.get("X-Dashboard-Token") == DASHBOARD_TOKEN

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({
            "ok": True,
            "paper_trading": True,
            "real_trading": False,
            "ws_connected": self.ws_connected,
            "new_entries_enabled": self.trading_enabled,
            "stop_loss_enabled": STOP_LOSS_ENABLED,
            "stop_loss_price": STOP_LOSS_PRICE,
            "tracked_markets": len(self.markets_by_yes),
            "time": iso_now(),
        })

    async def api_status_handler(self, request: web.Request) -> web.Response:
        if not self.dashboard_allowed(request):
            raise web.HTTPUnauthorized()
        events, markets = self.db.active_counts()
        return web.json_response({
            "paper_trading": True,
            "real_trading": False,
            "active_events": events,
            "active_yes_markets": markets,
            "ws_connected": self.ws_connected,
            "new_entries_enabled": self.trading_enabled,
            "stop_loss_enabled": STOP_LOSS_ENABLED,
            "stop_loss_price": STOP_LOSS_PRICE,
            "last_ws_message_at": self.last_ws_message_at,
            "last_discovery_at": self.last_discovery_at,
            "strategies": self.all_stats(),
        })

    async def dashboard_handler(self, request: web.Request) -> web.Response:
        if not self.dashboard_allowed(request):
            return web.Response(status=401, text="Unauthorized. Add ?token=YOUR_DASHBOARD_TOKEN")
        events, markets = self.db.active_counts()
        cards = []
        for s in self.all_stats():
            cards.append(
                f"""
                <section class="card">
                  <h2>{html.escape(str(s['strategy']))} · entry {s['threshold']:.2f}</h2>
                  <div class="metric">Equity <b>${s['equity']:.2f}</b></div>
                  <div class="metric">Total PnL <b class="{'pos' if s['total_pnl'] >= 0 else 'neg'}">{s['total_pnl']:+.2f}</b></div>
                  <div class="metric">ROI <b>{s['roi_pct']:+.2f}%</b></div>
                  <div class="metric">Cash <b>${s['cash']:.2f}</b></div>
                  <div class="metric">Realized <b>{s['realized_pnl']:+.2f}</b></div>
                  <div class="metric">Unrealized <b>{s['unrealized_pnl']:+.2f}</b></div>
                  <div class="metric">Trades <b>{s['trades']}</b> · open {s['open_count']} · SL {s['stop_loss_count']}</div>
                  <div class="metric">W/L <b>{s['wins']}/{s['losses']}</b> · WR {s['win_rate']:.1f}%</div>
                  <div class="metric">Fees <b>${s['fees']:.4f}</b></div>
                  <div class="metric">Max DD <b>{s['max_drawdown_pct']:.2f}%</b></div>
                </section>
                """
            )

        rows_html = []
        for tr in self.db.latest_trades(40):
            pnl = "OPEN" if tr["pnl"] is None else f"{float(tr['pnl']):+.3f}"
            rows_html.append(
                "<tr>"
                f"<td>{tr['id']}</td><td>{html.escape(str(tr['strategy']))}</td>"
                f"<td>{html.escape(str(tr['event_title']))}</td>"
                f"<td>{html.escape(str(tr['temperature_label']))}</td>"
                f"<td>{float(tr['avg_fill_price']):.4f}</td><td>${float(tr['taker_fee']):.5f}</td>"
                f"<td>{html.escape(str(tr['status']))}</td><td>{pnl}</td>"
                "</tr>"
            )

        body = f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30"><title>Weather Paper Bot</title>
<style>
body{{font-family:system-ui,-apple-system,sans-serif;margin:0;background:#0b1020;color:#edf2ff}}
main{{max-width:1250px;margin:auto;padding:22px}} .top{{display:flex;gap:18px;flex-wrap:wrap;margin-bottom:18px}}
.badge{{background:#17203a;border:1px solid #2b385d;border-radius:999px;padding:8px 12px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:14px}}
.card{{background:#121a30;border:1px solid #283657;border-radius:14px;padding:16px}}
h1{{margin:0 0 14px}} h2{{margin-top:0}} .metric{{display:flex;justify-content:space-between;margin:7px 0;gap:12px}}
.pos{{color:#66e39a}} .neg{{color:#ff7c8a}} table{{width:100%;border-collapse:collapse;margin-top:18px;font-size:13px}}
th,td{{border-bottom:1px solid #283657;padding:8px;text-align:left;vertical-align:top}} .scroll{{overflow:auto}}
small{{color:#a8b4d4}} a{{color:#9fc2ff}}
</style></head><body><main>
<h1>🌡 Polymarket Highest Temperature — PAPER</h1>
<div class="top"><span class="badge">REAL trading: OFF</span><span class="badge">WS: {'CONNECTED' if self.ws_connected else 'DISCONNECTED'}</span>
<span class="badge">City/date events: {events}</span><span class="badge">YES markets: {markets}</span>
<span class="badge">Order: ${TRADE_NOTIONAL_USD:.2f}</span>
<span class="badge">Entries: {'RUNNING' if self.trading_enabled else 'STOPPED'}</span>
<span class="badge">SL: ${STOP_LOSS_PRICE:.2f}</span></div>
<div class="grid">{''.join(cards)}</div>
<div class="card" style="margin-top:14px"><b>Last discovery:</b> {html.escape(self.last_discovery_at or '—')} &nbsp; <b>Last WS:</b> {html.escape(self.last_ws_message_at or '—')}<br>
<small>Unrealized PnL is marked at best bid (liquidation-side mark). Page refreshes every 30 seconds.</small></div>
<div class="scroll"><table><thead><tr><th>ID</th><th>Strategy</th><th>Event</th><th>Outcome</th><th>Avg fill</th><th>Fee</th><th>Status</th><th>PnL</th></tr></thead>
<tbody>{''.join(rows_html)}</tbody></table></div>
</main></body></html>"""
        return web.Response(text=body, content_type="text/html")

    def make_web_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/", self.dashboard_handler)
        app.router.add_get("/healthz", self.health_handler)
        app.router.add_get("/api/status", self.api_status_handler)
        return app


async def run_server_and_bot() -> None:
    bot = WeatherPaperBot()
    app = bot.make_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("HTTP dashboard listening on 0.0.0.0:%d", PORT)
    try:
        await bot.start()
    finally:
        await runner.cleanup()


def main() -> None:
    try:
        asyncio.run(run_server_and_bot())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
