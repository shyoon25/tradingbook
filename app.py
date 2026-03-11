from __future__ import annotations

import csv
import io
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

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
                datetime.utcnow().isoformat(timespec="seconds") + "Z",
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
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
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


if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="127.0.0.1", port=5001)
