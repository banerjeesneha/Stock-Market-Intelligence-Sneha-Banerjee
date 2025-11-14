import streamlit as st
import pandas as pd
import sqlite3

# --- 1️⃣ Load data from SQLite ---
conn = sqlite3.connect("stock_data.db")
df = pd.read_sql("SELECT * FROM stock_prices", conn)
conn.close()

# --- 2️⃣ Sidebar options ---
tickers = df['Ticker'].unique().tolist()
selected_tickers = st.sidebar.multiselect("Select Tickers", tickers, default=tickers)

adjustment = st.sidebar.slider("Simulate % Change in Stock Price", -20, 20, 0)
ma_window = st.sidebar.slider("Moving Average Window (days)", 5, 50, 10)

# --- 3️⃣ Filter and apply what-if adjustment ---
df_filtered = df[df['Ticker'].isin(selected_tickers)].copy()
df_filtered['Adjusted Close'] = df_filtered['Close'] * (1 + adjustment/100)

# --- 4️⃣ Dashboard title ---
st.title("📈 Stock Market Intelligence Dashboard")
st.write(f"Data for selected tickers with {adjustment}% hypothetical adjustment")

# --- 5️⃣ Display table ---
st.dataframe(df_filtered[['Date','Ticker','Close','Adjusted Close','Volume']].sort_values(['Date','Ticker']))

# --- 6️⃣ Pivot for charts ---
df_pivot = df_filtered.pivot(index='Date', columns='Ticker', values='Adjusted Close')
df_ma = df_filtered.pivot(index='Date', columns='Ticker', values='Close').rolling(ma_window).mean()

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
