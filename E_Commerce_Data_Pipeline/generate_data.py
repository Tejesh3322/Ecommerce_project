import pandas as pd
import random
from faker import Faker
import os

fake = Faker('en_IN')  # Use Indian context for names if desired, or just 'en_US'

# Configuration
NUM_CUSTOMERS = 2000
NUM_PRODUCTS = 100
NUM_ORDERS = 5000
OUTPUT_DIR = "data/raw"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 1. Generate Customers ---
def generate_customers(n):
    data = []
    for i in range(1, n + 1):
        name = fake.name()
        # Introduce some dirty data
        if random.random() < 0.05:
            email = "invalid_email"
        elif random.random() < 0.05:
            email = None # Missing email
        else:
            email = fake.email()
            
        city = fake.city()
        data.append([i, name, email, city])
    
    # Introduce duplicates
    for _ in range(50):
        data.append(random.choice(data))
        
    df = pd.DataFrame(data, columns=['customer_id', 'name', 'email', 'city'])
    return df

# --- 2. Generate Products ---
def generate_products(n):
    categories = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports']
    data = []
    for i in range(101, 101 + n):
        cat = random.choice(categories)
        if cat == 'Electronics':
            name = fake.word().capitalize() + " " + random.choice(['Phone', 'Laptop', 'Watch', 'Tablet'])
            price = random.randint(5000, 100000)
        elif cat == 'Fashion':
            name = fake.word().capitalize() + " " + random.choice(['Shirt', 'Jeans', 'Shoes', 'Dress'])
            price = random.randint(500, 5000)
        else:
            name = fake.word().capitalize() + " " + "Item"
            price = random.randint(100, 2000)
            
        data.append([i, name, cat, price])
    
    df = pd.DataFrame(data, columns=['product_id', 'product_name', 'category', 'price'])
    return df

# --- 3. Generate Orders ---
def generate_orders(n, customer_ids, product_ids):
    data = []
    for i in range(5001, 5001 + n):
        cust_id = random.choice(customer_ids)
        prod_id = random.choice(product_ids)
        date = fake.date_between(start_date='-1y', end_date='today')
        quantity = random.randint(1, 5)
        
        data.append([i, cust_id, prod_id, date, quantity])
        
    df = pd.DataFrame(data, columns=['order_id', 'customer_id', 'product_id', 'order_date', 'quantity'])
    return df

print("Generating data...")

# Generate and Save
df_customers = generate_customers(NUM_CUSTOMERS)
df_products = generate_products(NUM_PRODUCTS)
df_orders = generate_orders(NUM_ORDERS, df_customers['customer_id'].unique(), df_products['product_id'].unique())

df_customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
df_products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
df_orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)

print(f"Data generated in {OUTPUT_DIR}/")
print(f"Customers: {len(df_customers)}")
print(f"Products: {len(df_products)}")
print(f"Orders: {len(df_orders)}")
