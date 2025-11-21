import os
import sqlite3
import pandas as pd
import streamlit as st
from datetime import date

try:
    import yfinance as yf
except ModuleNotFoundError:
    st.error("yfinance is not installed. Add it to requirements.txt and redeploy.")
    st.stop()

DB_FILE = "stock_data.db"
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
START_DATE = "2024-01-01"

# --- 1️⃣ Run ETL if DB is missing ---
if not os.path.exists(DB_FILE):
    st.warning("Database not found. Pulling latest stock data...")
    today = date.today().strftime("%Y-%m-%d")
    
    try:
        data = yf.download(TICKERS, start=START_DATE, end=today, auto_adjust=False, progress=False)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        st.stop()
    
    if data.empty:
        st.error("No data returned. Check tickers or connection.")
        st.stop()
    
    # Transform data
    df = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=False)
    
    # Save to SQLite
    conn = sqlite3.connect(DB_FILE)
    df.to_sql("stock_prices", conn, if_exists="replace", index=False)
    conn.close()
    
    st.success(f"ETL complete. Latest data through: {today}")

# --- 2️⃣ Load data from SQLite ---
conn = sqlite3.connect(DB_FILE)
df = pd.read_sql("SELECT * FROM stock_prices", conn)
conn.close()

# --- 3️⃣ Ensure required columns exist ---
required_cols = ['date', 'ticker', 'close', 'volume']
for col in required_cols:
    if col not in df.columns:
        st.error(f"Database missing required column: {col}. Please rerun ETL.")
        st.stop()

df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by='date', ascending=False)

# --- Sidebar inputs ---
tickers = df['ticker'].unique().tolist()
selected_tickers = st.sidebar.multiselect("Select Tickers", tickers, default=tickers)
adjustment = st.sidebar.slider("Simulate % Change in Stock Price", -20, 20, 0)
ma_window = st.sidebar.slider("Moving Average Window (days)", 5, 50, 10)

# --- Data filtering & adjustments ---
df_filtered = df[df['ticker'].isin(selected_tickers)].copy()
df_filtered['adjusted_close'] = df_filtered['close'] * (1 + adjustment / 100)

# --- Dashboard ---
st.title("📈 Stock Market Intelligence Dashboard")
st.write(f"Data for selected tickers with {adjustment}% hypothetical adjustment")

st.dataframe(df_filtered[['date','ticker','close','adjusted_close','volume']].sort_values(['date','ticker'], ascending=[False, True]))

df_pivot = df_filtered.pivot(index='date', columns='ticker', values='adjusted_close')
df_ma = df_filtered.pivot(index='date', columns='ticker', values='close').rolling(ma_window).mean()

st.subheader(f"Adjusted Close Price (with {adjustment}% simulation)")
st.line_chart(df_pivot)

st.subheader(f"{ma_window}-day Moving Average of Close Price")
st.line_chart(df_ma)

# --- Download button ---
csv = df_filtered.to_csv(index=False).encode('utf-8')
st.download_button("Download Filtered Data as CSV", data=csv, file_name='stock_data_filtered.csv', mime='text/csv')

# --- Footer ---
st.markdown("""
<div style='text-align: center; color: gray; font-size: 0.9em; margin-top: 100px;'>
🧠 Built by Sneha Banerjee | <a href="https://www.linkedin.com/in/sneha-banerjee/" target="_blank">LinkedIn</a><br>
📊 Data Sources: Yahoo Finance<br>
🛠 Tools: Python, SQLite, Streamlit, pandas, yfinance
</div>
""", unsafe_allow_html=True)
