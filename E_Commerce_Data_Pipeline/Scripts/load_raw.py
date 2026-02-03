import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://Tejesh:qwer@localhost/ecommerce_raw")
conn = engine.connect()

customers= pd.read_csv("E_Commerce_Data_Pipeline/data/raw/customers.csv")
products= pd.read_csv("E_Commerce_Data_Pipeline/data/raw/products.csv")
orders= pd.read_csv("E_Commerce_Data_Pipeline/data/raw/orders.csv")

customers.to_sql("raw_customers", conn, if_exists="append", index=False)
products.to_sql("raw_products", conn, if_exists="append", index=False)
orders.to_sql("raw_orders", conn, if_exists="append", index=False)

print("Raw data loaded successfully")

