import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Date
from datetime import datetime

engine = create_engine("mysql+pymysql://Tejesh:qwer@localhost/Tejesh")

# Create table if it doesn't exist
metadata = MetaData()
employees_table = Table(
    'employees', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('emp_id', Integer),
    Column('name', String(100)),
    Column('department', String(100)),
    Column('role', String(100)),
    Column('start_date', Date),
    Column('end_date', Date, nullable=True),
    Column('is_current', String(1))
)
metadata.create_all(engine)

conn = engine.connect()

today = datetime.today().date()

# Read CSV
df_csv = pd.read_csv("Employee_history/employee_roster.csv")

for _, row in df_csv.iterrows():
    emp_id = row['emp_id']
    name = row['name']
    dept = row['department']
    role = row['role']

    # Fetch current active record
    query = text("""
        SELECT * FROM employees
        WHERE emp_id = :emp_id AND is_current = 'Y'
    """)
    result = conn.execute(query, {"emp_id": emp_id}).fetchone()

    # ------------------ CASE 1: New Employee ------------------
    if result is None:
        insert_query = text("""
            INSERT INTO employees
            (emp_id, name, department, role, start_date, end_date, is_current)
            VALUES (:emp_id, :name, :dept, :role, :start_date, NULL, 'Y')
        """)
        conn.execute(insert_query, {
            "emp_id": emp_id,
            "name": name,
            "dept": dept,
            "role": role,
            "start_date": today
        })

    # ------------------ CASE 2: Existing Employee ------------------
    else:
        old_dept = result.department
        old_role = result.role

        # Check if anything changed
        if old_dept != dept or old_role != role:

            # Close old record
            update_query = text("""
                UPDATE employees
                SET end_date = :end_date,
                    is_current = 'N'
                WHERE emp_id = :emp_id
                AND is_current = 'Y'
            """)
            conn.execute(update_query, {
                "end_date": today,
                "emp_id": emp_id
            })

            # Insert new record
            insert_query = text("""
                INSERT INTO employees
                (emp_id, name, department, role, start_date, end_date, is_current)
                VALUES (:emp_id, :name, :dept, :role, :start_date, NULL, 'Y')
            """)
            conn.execute(insert_query, {
                "emp_id": emp_id,
                "name": name,
                "dept": dept,
                "role": role,
                "start_date": today
            })

        # ------------------ CASE 3: No Change (Idempotent) ------------------
        else:
            pass  # Do nothing

conn.commit()
conn.close()

print("SCD Type 2 Employee Sync Completed!")
