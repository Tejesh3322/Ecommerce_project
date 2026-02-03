import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String

engine = create_engine("mysql+pymysql://Tejesh:qwer@localhost/Tejesh")

# ------------------ CREATE TABLES IF NOT EXIST ------------------
metadata = MetaData()

# Inventory Table
inventory_table = Table(
    'inventory_table', metadata,
    Column('product_id', Integer, primary_key=True),
    Column('product_name', String(100)),
    Column('stock', Integer)
)

# Sales Transactions Table (needed for the sales_query)
sales_table = Table(
    'sales_transactions', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_id', Integer),
    Column('quantity_sold', Integer),
    Column('transaction_date', String(50)) # Optional, but good to have
)

metadata.create_all(engine)
conn = engine.connect()

# ------------------ STEP 1: LOAD CSV ------------------
# Ensure CSV exists or handle error, but assuming it exists for now based on flow
try:
    df_stock = pd.read_csv("Wearhouse_stock/current_stock.csv")
except FileNotFoundError:
    # Fallback if running from a different directory or file missing
    try:
        df_stock = pd.read_csv("current_stock.csv")
    except FileNotFoundError:
        print("Error: current_stock.csv not found.")
        exit()

# ------------------ STEP 2: DATA CLEANING ------------------
# Remove rows with missing product_id or stock
df_stock = df_stock.dropna(subset=['product_id', 'stock'])

# Replace negative stock with 0
df_stock['stock'] = df_stock['stock'].apply(lambda x: max(x, 0))

# ------------------ STEP 3: LOAD SALES FROM DB ------------------
sales_query = text("""
SELECT product_id, SUM(quantity_sold) AS total_sold
FROM sales_transactions
GROUP BY product_id
""")
df_sales = pd.read_sql(sales_query, conn)

# ------------------ STEP 4: MERGE STOCK WITH SALES ------------------
df_merged = df_stock.merge(df_sales, on='product_id', how='left')

# If no sales, fill with 0
df_merged['total_sold'] = df_merged['total_sold'].fillna(0)

# ------------------ STEP 5: CALCULATE NEW INVENTORY ------------------
df_merged['new_stock'] = df_merged['stock'] - df_merged['total_sold']

# Prevent negative inventory after calculation
df_merged['new_stock'] = df_merged['new_stock'].apply(lambda x: max(x, 0))

# ------------------ STEP 6: UPDATE / INSERT INTO inventory_table ------------------
print(f"Processing {len(df_merged)} records...")

for _, row in df_merged.iterrows():
    product_id = row['product_id']
    name = row['product_name']
    new_stock = row['new_stock']

    # Check if product exists
    check_query = text("""
        SELECT * FROM inventory_table
        WHERE product_id = :pid
    """)
    result = conn.execute(check_query, {"pid": product_id}).fetchone()

    # If exists → UPDATE
    if result:
        update_query = text("""
            UPDATE inventory_table
            SET stock = :stock
            WHERE product_id = :pid
        """)
        conn.execute(update_query, {"stock": new_stock, "pid": product_id})
        # print(f"Updated Product {product_id} stock to {new_stock}")

    # If not exists → INSERT and log warning
    else:
        print(f"Product {product_id} not found in DB. Inserting new record.")

        insert_query = text("""
            INSERT INTO inventory_table (product_id, product_name, stock)
            VALUES (:pid, :name, :stock)
        """)
        conn.execute(insert_query, {
            "pid": product_id,
            "name": name,
            "stock": new_stock
        })

conn.commit()
conn.close()

print("Inventory Reconciliation Completed!")
