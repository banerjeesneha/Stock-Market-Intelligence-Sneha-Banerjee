import yfinance as yf
import pandas as pd
import sqlite3
from datetime import date

def run_etl():
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    
    # Use today's date for the end date
    today = date.today().strftime("%Y-%m-%d")
    
    # Pull data from Yahoo Finance
    data = yf.download(
        tickers,
        start="2024-01-01",
        end=today,
        auto_adjust=False,
        progress=False
    )

    if data.empty:
        print("❌ No data returned. Check tickers or internet connection.")
        return

    # Flatten MultiIndex
    df = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Sort by date descending (most recent first)
    df = df.sort_values(by='date', ascending=False)

    # Save to SQLite
    conn = sqlite3.connect("stock_data.db")
    df.to_sql("stock_prices", conn, if_exists="replace", index=False)
    conn.close()

    print(f"✅ ETL complete. Latest data through: {today}")
    print(f"📈 Rows written: {len(df)}")

if __name__ == "__main__":
    run_etl()
