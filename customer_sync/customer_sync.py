"""
Customer Profile Sync - SCD Type 1 & CRUD Operations
Connects to MySQL database on localhost
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text

# ------------------ DB CONNECTION (MySQL) ------------------
# Update 'customer_db' to your actual database name if different
DATABASE_NAME = "Tejesh"

# First, create the database if it doesn't exist
temp_engine = create_engine("mysql+pymysql://Tejesh:qwer@localhost/")
with temp_engine.connect() as temp_conn:
    temp_conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"))
    temp_conn.commit()
temp_engine.dispose()

# Now connect to the database
engine = create_engine(f"mysql+pymysql://Tejesh:qwer@localhost/{DATABASE_NAME}")
conn = engine.connect()

# ------------------ CREATE TABLE IF NOT EXISTS ------------------
with engine.begin() as connection:
    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL,
            phone_number VARCHAR(20) NOT NULL
        )
    """))

metadata = MetaData()
customers_table = Table('customers', metadata, autoload_with=engine)

# ------------------ READ CSV ------------------
df_updates = pd.read_csv("customer_sync\customer_updates.csv")
print(f"Loaded {len(df_updates)} records from CSV")

# ------------------ READ EXISTING DB DATA ------------------
df_db = pd.read_sql("SELECT * FROM customers", conn)
print(f"Existing customers in database: {len(df_db)}")

# ------------------ MERGE CSV WITH DB ------------------
df_merged = df_updates.merge(
    df_db,
    on="customer_id",
    how="left",
    suffixes=("_new", "_old")
)

# ------------------ IDENTIFY NEW CUSTOMERS ------------------
df_new = df_merged[df_merged['email_old'].isna()]
print(f"\nNew customers to INSERT: {len(df_new)}")

# ------------------ IDENTIFY CHANGED CUSTOMERS (SCD TYPE 1) ------------------
df_changed = df_merged[
    (df_merged['email_old'].notna()) &
    (
        (df_merged['name_new'] != df_merged['name_old']) |
        (df_merged['email_new'] != df_merged['email_old']) |
        (df_merged['phone_number_new'] != df_merged['phone_number_old'])
    )
]
print(f"Changed customers to UPDATE: {len(df_changed)}")

# ------------------ INSERT NEW CUSTOMERS ------------------
for _, row in df_new.iterrows():
    insert_query = customers_table.insert().values(
        customer_id=row['customer_id'],
        name=row['name_new'],
        email=row['email_new'],
        phone_number=row['phone_number_new']
    )
    conn.execute(insert_query)
    print(f"  INSERT: Customer ID {row['customer_id']} - {row['name_new']}")

# ------------------ UPDATE CHANGED CUSTOMERS ------------------
for _, row in df_changed.iterrows():
    update_query = (
        customers_table.update()
        .where(customers_table.c.customer_id == row['customer_id'])
        .values(
            name=row['name_new'],
            email=row['email_new'],
            phone_number=row['phone_number_new']
        )
    )
    conn.execute(update_query)
    print(f"  UPDATE: Customer ID {row['customer_id']} - Overwrote with new values")

# ------------------ COMMIT & CLOSE ------------------
conn.commit()
conn.close()

print("\nSCD Type 1 Sync Completed Successfully!")
