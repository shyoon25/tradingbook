# Trading Book (Python)

Simple Flask + SQLite trade journal with stats, equity curve, drawdown, and CSV import/export.

## Features
- Add/delete trades
- Filters by date/symbol/setup
- Metrics: win rate, net P&L, profit factor, avg R multiple
- Equity + drawdown chart
- CSV import/export

## Run
```bash
cd trading-book
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open http://127.0.0.1:5001

## CSV Import Columns
Required:
- `trade_date` (`YYYY-MM-DD`)
- `symbol`
- `side` (`BUY` or `SELL`)
- `entry_price`
- `exit_price`
- `quantity`

Optional:
- `fees`
- `setup_tag`
- `risk_amount`
- `notes`
# tradingbook
