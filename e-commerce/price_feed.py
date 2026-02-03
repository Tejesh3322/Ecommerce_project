import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Date, DateTime
from datetime import datetime

# ------------------ DB CONNECTION ------------------
engine = create_engine("mysql+pymysql://Tejesh:qwer@localhost/Tejesh")

# ------------------ CREATE TABLE IF NOT EXISTS ------------------
metadata = MetaData()
product_prices = Table(
    'product_prices', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_id', Integer),
    Column('product_name', String(100)),
    Column('category', String(50)),
    Column('price', Float),
    Column('start_date', Date),
    Column('end_date', Date, nullable=True),
    Column('is_current', String(1)),
    Column('last_updated_at', DateTime)
)

metadata.create_all(engine)
conn = engine.connect()

today = datetime.today().date()
now = datetime.now()

# ------------------ STEP 1: READ CSV ------------------
# Handling potential file path differences
try:
    df = pd.read_csv("e-commerce/price_feed.csv")
except FileNotFoundError:
    try:
        df = pd.read_csv("price_feed.csv")
    except FileNotFoundError:
        print("Error: price_feed.csv not found.")
        exit()

print(f"Loaded {len(df)} records from CSV.")

for _, row in df.iterrows():
    pid = row['product_id']
    name = row['product_name']
    category = row['category']   # Standard / Premium
    price = row['price']

    # Fetch current active record
    query = text("""
        SELECT * FROM product_prices
        WHERE product_id = :pid AND is_current = 'Y'
    """)
    result = conn.execute(query, {"pid": pid}).fetchone()

    # ------------------ NEW PRODUCT ------------------
    if result is None:
        insert_query = text("""
            INSERT INTO product_prices
            (product_id, product_name, category, price,
             start_date, end_date, is_current, last_updated_at)
            VALUES (:pid, :name, :cat, :price,
                    :start, NULL, 'Y', :updated)
        """)
        conn.execute(insert_query, {
            "pid": pid,
            "name": name,
            "cat": category,
            "price": price,
            "start": today,
            "updated": now
        })
        print(f"Inserted new product: {name}")

    # ------------------ EXISTING PRODUCT ------------------
    else:
        old_price = result.price

        # If price did not change → only update timestamp
        if old_price == price:
            update_time = text("""
                UPDATE product_prices
                SET last_updated_at = :updated
                WHERE product_id = :pid AND is_current = 'Y'
            """)
            conn.execute(update_time, {"updated": now, "pid": pid})
            # print(f"No price change for {name}, updated timestamp.")

        # ----------- STANDARD → SCD TYPE 1 -----------
        elif category == "Standard":
            update_query = text("""
                UPDATE product_prices
                SET price = :price,
                    last_updated_at = :updated
                WHERE product_id = :pid AND is_current = 'Y'
            """)
            conn.execute(update_query, {
                "price": price,
                "updated": now,
                "pid": pid
            })
            print(f"Updated price (Standard) for {name}")

        # ----------- PREMIUM → SCD TYPE 2 -----------
        elif category == "Premium":
            # Close old record
            close_query = text("""
                UPDATE product_prices
                SET end_date = :end,
                    is_current = 'N',
                    last_updated_at = :updated
                WHERE product_id = :pid AND is_current = 'Y'
            """)
            conn.execute(close_query, {
                "end": today,
                "updated": now,
                "pid": pid
            })

            # Insert new record
            insert_query = text("""
                INSERT INTO product_prices
                (product_id, product_name, category, price,
                 start_date, end_date, is_current, last_updated_at)
                VALUES (:pid, :name, :cat, :price,
                        :start, NULL, 'Y', :updated)
            """)
            conn.execute(insert_query, {
                "pid": pid,
                "name": name,
                "cat": category,
                "price": price,
                "start": today,
                "updated": now
            })
            print(f"Updated price (Premium - New Record) for {name}")

conn.commit()
conn.close()

print("Price Audit Completed!")
