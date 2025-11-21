import yfinance as yf
import pandas as pd
import sqlite3
import datetime

def run_etl():
    # 1️⃣ Tickers
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']

    # 2️⃣ Dynamic end date (today)
    today = pd.Timestamp(datetime.date.today())

    # 3️⃣ Download data
    data = yf.download(
        tickers,
        start="2024-01-01",
        end=today + pd.Timedelta(days=1),  # include today
        auto_adjust=False
    )

    if data.empty:
        print("❌ No data returned. Check tickers or internet connection.")
        return

    # 4️⃣ Flatten MultiIndex
    df = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()

    # Column names
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # 5️⃣ Save to SQLite
    conn = sqlite3.connect("stock_data.db")
    df.to_sql("stock_prices", conn, if_exists="replace", index=False)
    conn.close()

    print(f"✅ ETL complete. Updated through: {today.date()}")
    print(f"📈 Rows written: {len(df)}")

if __name__ == "__main__":
    run_etl()
