import streamlit as st
import pandas as pd
import sqlite3
import datetime
import yfinance as yf

# --- 0️⃣ Optional: Run ETL to refresh data ---
def run_etl():
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    start_date = "2024-01-01"
    today = pd.Timestamp(datetime.date.today())  # ✅ pandas Timestamp

    if pd.to_datetime(start_date) > today:
        print("❌ Start date is in the future.")
        return

    data = yf.download(tickers, start=start_date, end=today.strftime("%Y-%m-%d"), auto_adjust=False)

    if data.empty:
        print("❌ No data returned.")
        return

    df = data.stack(level=1).rename_axis(['Date', 'Ticker']).reset_index()
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    conn = sqlite3.connect("stock_data.db")
    df.to_sql("stock_prices", conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ ETL complete. Updated through: {today.date()}")
    print(f"📈 Rows written: {len(df)}")

# --- 1️⃣ Load data from SQLite ---
conn = sqlite3.connect("stock_data.db")
df = pd.read_sql("SELECT * FROM stock_prices", conn)
conn.close()

# Convert all columns to lowercase to avoid KeyError
df.columns = [col.lower() for col in df.columns]

# --- 2️⃣ Sidebar options ---
tickers = df['ticker'].unique().tolist()
selected_tickers = st.sidebar.multiselect("Select Tickers", tickers, default=tickers)

adjustment = st.sidebar.slider("Simulate % Change in Stock Price", -20, 20, 0)
ma_window = st.sidebar.slider("Moving Average Window (days)", 5, 50, 10)

# --- 3️⃣ Filter and apply what-if adjustment ---
df_filtered = df[df['ticker'].isin(selected_tickers)].copy()
df_filtered['adjusted_close'] = df_filtered['close'] * (1 + adjustment/100)

# --- 4️⃣ Dashboard title ---
st.title("📈 Stock Market Intelligence Dashboard")
st.write(f"Data for selected tickers with {adjustment}% hypothetical adjustment")

# --- 5️⃣ Display table ---
st.dataframe(df_filtered[['date','ticker','close','adjusted_close','volume']].sort_values(['date','ticker']))

# --- 6️⃣ Pivot for charts ---
df_pivot = df_filtered.pivot(index='date', columns='ticker', values='adjusted_close')
df_ma = df_filtered.pivot(index='date', columns='ticker', values='close').rolling(ma_window).mean()

# --- 7️⃣ Line charts ---
st.subheader(f"Adjusted Close Price (with {adjustment}% simulation)")
st.line_chart(df_pivot)

st.subheader(f"{ma_window}-day Moving Average of Close Price")
st.line_chart(df_ma)

# --- 8️⃣ Download CSV button ---
csv = df_filtered.to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download Filtered Data as CSV",
    data=csv,
    file_name='stock_data_filtered.csv',
    mime='text/csv',
)

# --- 9️⃣ Footer ---
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 0.9em; margin-top: 100px;'>
        🧠 Built by Sneha Banerjee | 
        <a href="https://www.linkedin.com/in/sneha-banerjee/" target="_blank">LinkedIn</a>
        <br>
        📊 Data Sources: Yahoo Finance
        <br>
        🛠 Tools: Python, SQLite, Streamlit, pandas, yfinance
    </div>
    """,
    unsafe_allow_html=True
)
