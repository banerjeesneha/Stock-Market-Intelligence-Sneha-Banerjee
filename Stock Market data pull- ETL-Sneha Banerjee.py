import yfinance as yf
import pandas as pd
import sqlite3
import datetime

def run_etl():
    # -----------------------------
    # 1. Tickers
    # -----------------------------
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']

    # -----------------------------
    # 2. Dynamic end date (today)
    # -----------------------------
    today = datetime.date.today().strftime("%Y-%m-%d")

    # -----------------------------
    # 3. Download data
    # -----------------------------
    data = yf.download(
        tickers,
        start="2024-01-01",
        end=today,
        auto_adjust=False
    )

    # Handle empty result
    if data.empty:
        print("❌ No data returned. Check tickers or internet connection.")
        return

    # -----------------------------
    # 4. Flatten MultiIndex
    # -----------------------------
    df = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()

    # Ensure consistent column names
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # -----------------------------
    # 5. Save to SQLite
    # -----------------------------
    conn = sqlite3.connect("stock_data.db")

    df.to_sql("stock_prices", conn, if_exists="replace", index=False)

    conn.close()

    print(f"✅ ETL complete. Updated through: {today}")
    print(f"📈 Rows written: {len(df)}")


if __name__ == "__main__":
    run_etl()
