import sqlite3
import pandas as pd
import time


# Database setup
def setup_database():
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  symbol TEXT,
                  buy_price REAL,
                  quantity INTEGER)''')
    conn.commit()
    conn.close()


# Insert buy order into database
def insert_buy_order(symbol, buy_price, quantity):
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO trades (symbol, buy_price, quantity) VALUES (?, ?, ?)",
              (symbol, buy_price, quantity))
    conn.commit()
    conn.close()


# Fetch buy orders from database
def fetch_buy_orders():
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT symbol, buy_price, quantity FROM trades")
    rows = c.fetchall()
    conn.close()
    return rows


# Placeholder for a function to place trades
def placeTrades(symbol, trade_type, quantity):
    # Implement your trade placing logic here
    print(f"Placing {trade_type} order for {symbol} of quantity {quantity}")


# Placeholder for fetching current price of a symbol
def fetchCurrentPrice(symbol):
    # Replace this with actual logic to fetch current price
    current_prices = {
        "HNGSNGBEES": 310.00,  # Example current price
        "OTHER_SYMBOL": 312.00
    }
    return current_prices.get(symbol, None)


# Function to monitor prices and execute sell orders
def monitorAndSell(df):
    while True:
        for index, row in df.iterrows():
            symbol = row['symbol']
            buy_price = row['buy_price']
            quantity = row['quantity']
            target_price = buy_price * 1.06

            current_price = fetchCurrentPrice(symbol)
            if current_price and current_price >= target_price:
                placeTrades(symbol, "SELL", quantity)

                # Remove the sold row from the DataFrame and database
                df.drop(index, inplace=True)
                remove_order_from_db(symbol)

        # Break the loop if all trades are executed
        if df.empty:
            break

        # Wait for a specified interval before checking prices again
        time.sleep(60)


# Remove sold order from database
def remove_order_from_db(symbol):
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("DELETE FROM trades WHERE symbol = ?", (symbol,))
    conn.commit()
    conn.close()


# Main function to place buy orders and monitor prices
def main():
    setup_database()

    # Fetch buy orders from database to repopulate the DataFrame
    buy_orders = fetch_buy_orders()
    data = [{"symbol": row[0], "buy_price": row[1], "quantity": row[2]} for row in buy_orders]
    df = pd.DataFrame(data)

    print("Fetched from db",df)
    if df.empty:
        # Place initial buy trades
        initial_trades = [
            {"symbol": "HNGSNGBEES", "buy_price": 290.00, "quantity": 100},
            {"symbol": "OTHER_SYMBOL", "buy_price": 295.00, "quantity": 100}
        ]
        for trade in initial_trades:
            symbol = trade["symbol"]
            buy_price = trade["buy_price"]
            quantity = trade["quantity"]
            placeTrades(symbol, "BUY", quantity)
            insert_buy_order(symbol, buy_price, quantity)
            df = df._append(trade, ignore_index=True)

    # Start monitoring prices and executing sell orders
    monitorAndSell(df)


# if __name__ == "__main__":
#     main()
