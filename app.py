from __future__ import annotations

import csv
import io
import random
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error, request as urlrequest

from flask import Flask, jsonify, render_template, request, send_file

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "trading_book.db"

app = Flask(__name__)


CREATE_TRADES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY', 'SELL')),
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    quantity REAL NOT NULL,
    fees REAL NOT NULL DEFAULT 0,
    setup_tag TEXT DEFAULT '',
    risk_amount REAL,
    notes TEXT DEFAULT '',
    created_at TEXT NOT NULL
);
"""

DEFAULT_MARKET_SEED_SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "SPY", "QQQ"]
MARKET_SEED_SETUPS = ["Breakout", "Pullback", "Reversal", "Opening Range", "Trend Continuation", "VWAP Reclaim"]


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.execute(CREATE_TRADES_TABLE_SQL)
        conn.commit()


def validate_date(date_text: str) -> str:
    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("trade_date must be in YYYY-MM-DD format") from exc
    return dt.strftime("%Y-%m-%d")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_date_input(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def normalize_trade_input(payload: dict[str, Any]) -> dict[str, Any]:
    required_fields = ["trade_date", "symbol", "side", "entry_price", "exit_price", "quantity"]
    missing = [field for field in required_fields if field not in payload or str(payload[field]).strip() == ""]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    trade_date = validate_date(str(payload["trade_date"]))
    symbol = str(payload["symbol"]).strip().upper()
    side = str(payload["side"]).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    try:
        entry_price = float(payload["entry_price"])
        exit_price = float(payload["exit_price"])
        quantity = float(payload["quantity"])
        fees = float(payload.get("fees", 0) or 0)
        risk_amount = payload.get("risk_amount")
        risk_amount = float(risk_amount) if risk_amount not in (None, "") else None
    except (TypeError, ValueError) as exc:
        raise ValueError("entry_price, exit_price, quantity, fees, and risk_amount must be numbers") from exc

    if entry_price <= 0 or exit_price <= 0 or quantity <= 0:
        raise ValueError("entry_price, exit_price, and quantity must be positive")

    return {
        "trade_date": trade_date,
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "quantity": quantity,
        "fees": fees,
        "setup_tag": str(payload.get("setup_tag", "")).strip(),
        "risk_amount": risk_amount,
        "notes": str(payload.get("notes", "")).strip(),
    }


def compute_pnl(side: str, entry_price: float, exit_price: float, quantity: float, fees: float) -> float:
    if side == "BUY":
        gross = (exit_price - entry_price) * quantity
    else:
        gross = (entry_price - exit_price) * quantity
    return gross - fees


def row_to_trade(row: sqlite3.Row) -> dict[str, Any]:
    trade = dict(row)
    trade["pnl"] = compute_pnl(
        trade["side"],
        float(trade["entry_price"]),
        float(trade["exit_price"]),
        float(trade["quantity"]),
        float(trade["fees"]),
    )
    risk = trade.get("risk_amount")
    trade["r_multiple"] = trade["pnl"] / risk if risk not in (None, 0) else None
    return trade


def build_filters() -> tuple[str, list[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []

    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    symbol = request.args.get("symbol", "").strip().upper()
    setup_tag = request.args.get("setup_tag", "").strip()

    if start_date:
        where_parts.append("trade_date >= ?")
        params.append(validate_date(start_date))
    if end_date:
        where_parts.append("trade_date <= ?")
        params.append(validate_date(end_date))
    if symbol:
        where_parts.append("symbol = ?")
        params.append(symbol)
    if setup_tag:
        where_parts.append("setup_tag = ?")
        params.append(setup_tag)

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return where_clause, params


def fetch_filtered_trades() -> list[dict[str, Any]]:
    where_clause, params = build_filters()
    query = f"""
        SELECT id, trade_date, symbol, side, entry_price, exit_price, quantity, fees, setup_tag, risk_amount, notes, created_at
        FROM trades
        {where_clause}
        ORDER BY trade_date ASC, id ASC
    """
    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
    return [row_to_trade(row) for row in rows]


def validate_symbols(symbols: list[str]) -> list[str]:
    clean: list[str] = []
    for raw in symbols:
        symbol = raw.strip().upper()
        if not symbol:
            continue
        if not re.fullmatch(r"[A-Z0-9.\-]{1,10}", symbol):
            raise ValueError(f"Invalid symbol: {raw}")
        clean.append(symbol)
    if not clean:
        raise ValueError("At least one symbol is required")
    return list(dict.fromkeys(clean))


def fetch_stooq_daily(symbol: str) -> dict[str, dict[str, float]]:
    stooq_symbol = f"{symbol.lower()}.us"
    url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"
    req = urlrequest.Request(url, headers={"User-Agent": "trading-book/1.0"})
    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            csv_text = resp.read().decode("utf-8", errors="replace")
    except (error.HTTPError, error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"Failed to fetch market data for {symbol}") from exc

    rows: dict[str, dict[str, float]] = {}
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        try:
            trade_date = validate_date(row["Date"])
            o = float(row["Open"])
            h = float(row["High"])
            l = float(row["Low"])
            c = float(row["Close"])
        except (TypeError, ValueError, KeyError):
            continue
        if min(o, h, l, c) <= 0:
            continue
        rows[trade_date] = {"open": o, "high": h, "low": l, "close": c}
    if not rows:
        raise RuntimeError(f"No market data returned for {symbol}")
    return rows


def generate_market_seed_trades(
    symbols: list[str],
    start_date: str,
    end_date: str,
    max_trades_per_day: int,
) -> list[tuple[Any, ...]]:
    start = parse_date_input(start_date, "start_date")
    end = parse_date_input(end_date, "end_date")
    if start > end:
        raise ValueError("start_date must be on or before end_date")
    if max_trades_per_day < 1 or max_trades_per_day > 10:
        raise ValueError("max_trades_per_day must be between 1 and 10")

    market_by_symbol: dict[str, dict[str, dict[str, float]]] = {}
    for symbol in symbols:
        market_by_symbol[symbol] = fetch_stooq_daily(symbol)

    rng = random.Random()
    day = start
    inserts: list[tuple[Any, ...]] = []
    while day <= end:
        day_str = day.isoformat()
        available_symbols = [s for s in symbols if day_str in market_by_symbol[s]]
        if available_symbols:
            trades_today = rng.randint(1, min(max_trades_per_day, len(available_symbols)))
            picked = rng.sample(available_symbols, k=trades_today)
            for symbol in picked:
                bar = market_by_symbol[symbol][day_str]
                side = rng.choice(["BUY", "SELL"])
                slip_in = rng.uniform(-0.0008, 0.0008)
                slip_out = rng.uniform(-0.0008, 0.0008)
                entry = round(bar["open"] * (1 + slip_in), 4)
                exit_price = round(bar["close"] * (1 + slip_out), 4)
                quantity = float(rng.choice([5, 10, 15, 20, 25, 30, 50]))
                fees = round(rng.uniform(0.5, 4.0), 2)
                intraday_range = max(0.01, bar["high"] - bar["low"])
                risk_amount = round(max(10.0, intraday_range * quantity * rng.uniform(0.4, 0.9)), 2)
                notes = "Seeded from STOOQ daily OHLC data"
                inserts.append(
                    (
                        day_str,
                        symbol,
                        side,
                        entry,
                        exit_price,
                        quantity,
                        fees,
                        rng.choice(MARKET_SEED_SETUPS),
                        risk_amount,
                        notes,
                        now_utc_iso(),
                    )
                )
        day += timedelta(days=1)

    return inserts


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/trades")
def list_trades():
    try:
        return jsonify({"trades": fetch_filtered_trades()})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.post("/api/trades")
def create_trade():
    payload = request.get_json(silent=True) or {}
    try:
        trade = normalize_trade_input(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    with get_db() as conn:
        cursor = conn.execute(
            """
            INSERT INTO trades (
                trade_date, symbol, side, entry_price, exit_price, quantity,
                fees, setup_tag, risk_amount, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade["trade_date"],
                trade["symbol"],
                trade["side"],
                trade["entry_price"],
                trade["exit_price"],
                trade["quantity"],
                trade["fees"],
                trade["setup_tag"],
                trade["risk_amount"],
                trade["notes"],
                now_utc_iso(),
            ),
        )
        conn.commit()
        trade_id = cursor.lastrowid
        row = conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()

    return jsonify({"trade": row_to_trade(row)}), 201


@app.delete("/api/trades/<int:trade_id>")
def delete_trade(trade_id: int):
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
        conn.commit()
    if cursor.rowcount == 0:
        return jsonify({"error": "Trade not found"}), 404
    return jsonify({"ok": True})


@app.get("/api/stats")
def stats():
    try:
        trades = fetch_filtered_trades()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    total = len(trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    losses = sum(1 for t in trades if t["pnl"] < 0)
    breakeven = total - wins - losses
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = sum(t["pnl"] for t in trades if t["pnl"] < 0)
    net_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = net_pnl / total if total else 0
    win_rate = (wins / total * 100) if total else 0
    profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else None

    r_values = [t["r_multiple"] for t in trades if t["r_multiple"] is not None]
    avg_r = (sum(r_values) / len(r_values)) if r_values else None

    return jsonify(
        {
            "total_trades": total,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate": win_rate,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "net_pnl": net_pnl,
            "avg_pnl": avg_pnl,
            "profit_factor": profit_factor,
            "avg_r_multiple": avg_r,
        }
    )


@app.get("/api/equity")
def equity():
    try:
        trades = fetch_filtered_trades()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    curve = []
    equity_value = 0.0
    peak = 0.0

    for trade in trades:
        equity_value += trade["pnl"]
        peak = max(peak, equity_value)
        drawdown = equity_value - peak
        curve.append(
            {
                "id": trade["id"],
                "trade_date": trade["trade_date"],
                "equity": equity_value,
                "drawdown": drawdown,
            }
        )

    return jsonify({"curve": curve})


@app.get("/api/export.csv")
def export_csv():
    try:
        trades = fetch_filtered_trades()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "id",
            "trade_date",
            "symbol",
            "side",
            "entry_price",
            "exit_price",
            "quantity",
            "fees",
            "setup_tag",
            "risk_amount",
            "notes",
            "pnl",
            "r_multiple",
            "created_at",
        ]
    )
    for trade in trades:
        writer.writerow(
            [
                trade["id"],
                trade["trade_date"],
                trade["symbol"],
                trade["side"],
                trade["entry_price"],
                trade["exit_price"],
                trade["quantity"],
                trade["fees"],
                trade["setup_tag"],
                trade["risk_amount"],
                trade["notes"],
                trade["pnl"],
                trade["r_multiple"],
                trade["created_at"],
            ]
        )

    buffer = io.BytesIO(output.getvalue().encode("utf-8"))
    output.close()
    return send_file(
        buffer,
        as_attachment=True,
        download_name="trades_export.csv",
        mimetype="text/csv",
    )


@app.post("/api/import.csv")
def import_csv():
    if "file" not in request.files:
        return jsonify({"error": "Missing file field"}), 400

    file = request.files["file"]
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Upload a CSV file"}), 400

    content = file.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    required_cols = {"trade_date", "symbol", "side", "entry_price", "exit_price", "quantity"}

    if not reader.fieldnames or not required_cols.issubset(set(reader.fieldnames)):
        return jsonify({"error": f"CSV must include columns: {', '.join(sorted(required_cols))}"}), 400

    rows_to_insert = []
    line_no = 1
    try:
        for row in reader:
            line_no += 1
            trade = normalize_trade_input(row)
            rows_to_insert.append(
                (
                    trade["trade_date"],
                    trade["symbol"],
                    trade["side"],
                    trade["entry_price"],
                    trade["exit_price"],
                    trade["quantity"],
                    trade["fees"],
                    trade["setup_tag"],
                    trade["risk_amount"],
                    trade["notes"],
                    now_utc_iso(),
                )
            )
    except ValueError as exc:
        return jsonify({"error": f"Row {line_no}: {exc}"}), 400

    with get_db() as conn:
        conn.executemany(
            """
            INSERT INTO trades (
                trade_date, symbol, side, entry_price, exit_price, quantity,
                fees, setup_tag, risk_amount, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()

    return jsonify({"imported": len(rows_to_insert)})


@app.post("/api/seed/market")
def seed_market_data():
    payload = request.get_json(silent=True) or {}
    raw_symbols = payload.get("symbols", DEFAULT_MARKET_SEED_SYMBOLS)
    if isinstance(raw_symbols, str):
        raw_symbols = [part.strip() for part in raw_symbols.split(",")]
    if not isinstance(raw_symbols, list):
        return jsonify({"error": "symbols must be a list or comma-separated string"}), 400

    try:
        symbols = validate_symbols([str(s) for s in raw_symbols])
        start_date = str(payload.get("start_date", "2025-09-01"))
        end_date = str(payload.get("end_date", datetime.now().date().isoformat()))
        max_trades_per_day = int(payload.get("max_trades_per_day", 2))
        clear_existing = bool(payload.get("clear_existing", False))
        rows_to_insert = generate_market_seed_trades(symbols, start_date, end_date, max_trades_per_day)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502

    if not rows_to_insert:
        return jsonify({"seeded": 0, "message": "No market bars found in date range"}), 200

    with get_db() as conn:
        if clear_existing:
            conn.execute("DELETE FROM trades")
        conn.executemany(
            """
            INSERT INTO trades (
                trade_date, symbol, side, entry_price, exit_price, quantity,
                fees, setup_tag, risk_amount, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()

    return jsonify({"seeded": len(rows_to_insert), "symbols": symbols})


if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="127.0.0.1", port=5001)
