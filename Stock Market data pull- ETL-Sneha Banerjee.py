import yfinance as yf
import pandas as pd
import sqlite3
import datetime

def run_etl():
    # -----------------------------
    # 1️⃣ Tickers
    # -----------------------------
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']

    # -----------------------------
    # 2️⃣ Dynamic dates
    # -----------------------------
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")

    # -----------------------------
    # 3️⃣ Connect to SQLite and find last date
    # -----------------------------
    conn = sqlite3.connect("stock_data.db")
    try:
        last_date = pd.read_sql("SELECT MAX(date) as last_date FROM stock_prices", conn).iloc[0,0]
        if last_date is not None:
            start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = "2024-01-01"
    except:
        start_date = "2024-01-01"

    # If DB already up-to-date
    if pd.to_datetime(start_date) > today:
        print("✅ Database already up-to-date!")
        conn.close()
        return

    # -----------------------------
    # 4️⃣ Download new data
    # -----------------------------
    data = yf.download(
        tickers,
        start=start_date,
        end=today_str,
        auto_adjust=False,
        progress=False
    )

    if data.empty:
        print("❌ No new data returned. Market may be closed today.")
        conn.close()
        return

    # -----------------------------
    # 5️⃣ Flatten MultiIndex
    # -----------------------------
    df_new = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()
    df_new.columns = [col.lower().replace(" ", "_") for col in df_new.columns]
    df_new['date'] = df_new['date'].astype(str)

    # -----------------------------
    # 6️⃣ Append new rows
    # -----------------------------
    df_new.to_sql("stock_prices", conn, if_exists="append", index=False)
    conn.close()

    print(f"✅ ETL complete. Appended data from {start_date} to {today_str}")
    print(f"📈 Rows added: {len(df_new)}")

if __name__ == "__main__":
    run_etl()
