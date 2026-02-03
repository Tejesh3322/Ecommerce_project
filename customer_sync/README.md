# Customer Profile Sync - SCD Type 1 & CRUD

A Python solution for synchronizing customer profile data from CSV files to a SQL database using Slowly Changing Dimension Type 1 (overwrite) strategy.

## Features

- **CSV Reading**: Uses Pandas to read `customer_updates.csv`
- **Change Detection**: Identifies records where email or phone_number has changed
- **SCD Type 1**: Overwrites existing records with new values (no history maintained)
- **CRUD Operations**: 
  - INSERT for new customers
  - UPDATE for existing customers with changes

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Place your `customer_updates.csv` file in the project directory with these columns:
   - `customer_id`
   - `name`
   - `email`
   - `phone_number`

2. Run the sync:
```bash
python customer_sync.py
```

## How It Works

1. **Load CSV**: Reads the customer updates file using Pandas
2. **Compare**: Fetches existing records and compares with CSV data
3. **Identify Changes**: Detects new customers and changed fields (email, phone)
4. **Execute CRUD**:
   - New customers → INSERT
   - Changed customers → UPDATE (SCD Type 1 overwrite)

## Database

Uses SQLite (`customers.db`) for easy local testing. The `Customer` table includes:
- `customer_id` (Primary Key)
- `name`
- `email`
- `phone_number`
- `created_at`
- `updated_at`
