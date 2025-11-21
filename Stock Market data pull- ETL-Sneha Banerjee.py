import yfinance as yf
import pandas as pd
import sqlite3
from datetime import date

DB_FILE = "stock_data.db"
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
START_DATE = "2024-01-01"

def run_etl():
    today = date.today().strftime("%Y-%m-%d")

    # Pull data from Yahoo Finance
    try:
        data = yf.download(
            TICKERS,
            start=START_DATE,
            end=today,
            auto_adjust=False,
            progress=False
        )
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return

    if data.empty:
        print("❌ No data returned. Check tickers or internet connection.")
        return

    # Flatten MultiIndex (if multiple tickers)
    df = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Ensure required columns exist
    required_cols = ['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Convert date to datetime and sort descending
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=False)

    # Save to SQLite
    try:
        conn = sqlite3.connect(DB_FILE)
        df.to_sql("stock_prices", conn, if_exists="replace", index=False)
        conn.close()
    except Exception as e:
        print(f"❌ Error saving to database: {e}")
        return

    print(f"✅ ETL complete. Latest data through: {today}")
    print(f"📈 Rows written: {len(df)}")

if __name__ == "__main__":
    run_etl()
