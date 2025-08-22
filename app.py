# # app.py
# import streamlit as st
# import psycopg2
# import pandas as pd

# st.title("📊 Stock Market Data Viewer")

# # Connect to Postgres
# conn = psycopg2.connect(
#     dbname="stock_data",
#     user="airflow",
#     password="airflow",   # use the password from docker-compose
#     host="localhost",     # works because port 5432 is mapped
#     port="5432"
# )

# # Query Data
# query = """
# SELECT symbol, date, open_price, close_price, volume
# FROM stock_market.daily_prices
# ORDER BY date DESC
# LIMIT 10;
# """
# df = pd.read_sql(query, conn)

# # Show table
# st.dataframe(df)

# # Show chart
# st.line_chart(df.set_index("date")[["open_price", "close_price"]])



import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# DB Connection Settings
DB_CONFIG = {
    "dbname": "stock_data",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",  # or "postgres" inside docker network
    "port": "5432",
}


# Function to fetch data
def fetch_data(symbol: str, limit: int = 10):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = f"""
            SET search_path TO stock_market;
            SELECT * FROM daily_prices
            WHERE symbol = %s
            ORDER BY date DESC
            LIMIT %s;
        """
        with conn.cursor() as cur:
            cur.execute("SET search_path TO stock_market;")
            cur.execute(
                """
                SELECT date, open_price, high_price, low_price, close_price, volume
                FROM daily_prices
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT %s;
                """,
                (symbol, limit),
            )
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
        conn.close()
        return pd.DataFrame(rows, columns=colnames)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Streamlit UI
st.title("📊 Stock Market Dashboard")

symbol = st.text_input("Enter Stock Symbol (e.g. AAPL):", "AAPL")
limit = st.slider("Number of records:", min_value=5, max_value=50, value=10)

if st.button("Fetch Data"):
    df = fetch_data(symbol, limit)
    if not df.empty:
        st.subheader(f"Last {limit} records for {symbol}")
        st.dataframe(df)

        # Chart
        st.subheader("Closing Price Trend")
        fig, ax = plt.subplots()
        ax.plot(df["date"], df["close_price"], marker="o")
        ax.set_xlabel("Date")
        ax.set_ylabel("Close Price")
        ax.set_title(f"{symbol} Closing Prices")
        st.pyplot(fig)
    else:
        st.warning("No data found.")
