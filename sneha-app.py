import streamlit as st
import pandas as pd
import sqlite3

# --- Load latest data from SQLite ---
conn = sqlite3.connect("stock_data.db")
df = pd.read_sql("SELECT * FROM stock_prices", conn)
conn.close()

# --- Sort by most recent first ---
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by='date', ascending=False)

# --- Sidebar options ---
tickers = df['ticker'].unique().tolist()
selected_tickers = st.sidebar.multiselect("Select Tickers", tickers, default=tickers)

adjustment = st.sidebar.slider("Simulate % Change in Stock Price", -20, 20, 0)
ma_window = st.sidebar.slider("Moving Average Window (days)", 5, 50, 10)

# --- Filter and apply what-if adjustment ---
df_filtered = df[df['ticker'].isin(selected_tickers)].copy()
df_filtered['adjusted_close'] = df_filtered['close'] * (1 + adjustment/100)

# --- Dashboard title ---
st.title("📈 Stock Market Intelligence Dashboard")
st.write(f"Data for selected tickers with {adjustment}% hypothetical adjustment")

# --- Display table ---
st.dataframe(
    df_filtered[['date','ticker','close','adjusted_close','volume']].sort_values(['date','ticker'], ascending=[False, True])
)

# --- Pivot for charts ---
df_pivot = df_filtered.pivot(index='date', columns='ticker', values='adjusted_close')
df_ma = df_filtered.pivot(index='date', columns='ticker', values='close').rolling(ma_window).mean()

# --- Line charts ---
st.subheader(f"Adjusted Close Price (with {adjustment}% simulation)")
st.line_chart(df_pivot)

st.subheader(f"{ma_window}-day Moving Average of Close Price")
st.line_chart(df_ma)

# --- Download CSV button ---
csv = df_filtered.to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download Filtered Data as CSV",
    data=csv,
    file_name='stock_data_filtered.csv',
    mime='text/csv',
)

# --- Footer ---
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 0.9em; margin-top: 100px;'>
        🧠 Built by Sneha Banerjee | <a href="https://www.linkedin.com/in/sneha-banerjee/" target="_blank">LinkedIn</a>
        <br>📊 Data Sources: Yahoo Finance
        <br>🛠 Tools: Python, SQLite, Streamlit, pandas, yfinance
    </div>
    """,
    unsafe_allow_html=True
)
