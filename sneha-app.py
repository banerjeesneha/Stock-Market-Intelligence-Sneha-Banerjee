import streamlit as st
import pandas as pd
import sqlite3
import datetime
import os
import subprocess

DB_PATH = "stock_data.db"

# --- 0️⃣ Optional: Auto-run ETL if DB not updated today ---
def is_db_up_to_date(db_path):
    if not os.path.exists(db_path):
        return False
    conn = sqlite3.connect(db_path)
    last_date = pd.read_sql("SELECT MAX(date) AS max_date FROM stock_prices", conn).iloc[0,0]
    conn.close()
    if last_date is None:
        return False
    return str(datetime.date.today()) <= str(last_date)

if not is_db_up_to_date(DB_PATH):
    st.info("Updating stock data...")
    subprocess.run(["python3", "Stock Market data pull- ETL-Sneha Banerjee.py"])
    st.success("Stock data updated!")

# --- 1️⃣ Load data from SQLite ---
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM stock_prices", conn)
conn.close()

# --- Ensure consistent column names ---
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
st.write(f"Latest Trading Date: {df_filtered['date'].max()}")

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
        🧠 Built by Sneha Banerjee | <a href="https://www.linkedin.com/in/sneha-banerjee/" target="_blank">LinkedIn</a><br>
        📊 Data Sources: Yahoo Finance<br>
        🛠 Tools: Python, SQLite, Streamlit, pandas, yfinance
    </div>
    """,
    unsafe_allow_html=True
)
