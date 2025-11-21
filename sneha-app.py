import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import datetime

# --- 0️⃣ Incremental ETL before loading data ---
def run_etl():
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")

    conn = sqlite3.connect("stock_data.db")
    try:
        last_date = pd.read_sql("SELECT MAX(date) as last_date FROM stock_prices", conn).iloc[0,0]
        if last_date is not None:
            start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = "2024-01-01"
    except:
        start_date = "2024-01-01"

    if pd.to_datetime(start_date) > today:
        conn.close()
        return

    data = yf.download(
        tickers,
        start=start_date,
        end=today_str,
        auto_adjust=False,
        progress=False
    )

    if not data.empty:
        df_new = data.stack(level=1).rename_axis(['date', 'ticker']).reset_index()
        df_new.columns = [col.lower().replace(" ", "_") for col in df_new.columns]
        df_new['date'] = df_new['date'].astype(str)
        df_new.to_sql("stock_prices", conn, if_exists="append", index=False)
    conn.close()

run_etl()

# --- 1️⃣ Load data from SQLite ---
conn = sqlite3.connect("stock_data.db")
df = pd.read_sql("SELECT * FROM stock_prices", conn)
conn.close()

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
        <a href="https://www.linkedin.com/in/sneha-banerjee/" target="_blank">LinkedIn</a><br>
        📊 Data Sources: Yahoo Finance<br>
        🛠 Tools: Python, SQLite, Streamlit, pandas, yfinance
    </div>
    """,
    unsafe_allow_html=True
)
