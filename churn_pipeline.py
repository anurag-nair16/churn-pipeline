import random
import pandas as pd
import mysql.connector
from faker import Faker
import os

# Connect to TiDB using environment variables
conn = mysql.connector.connect(
    host=os.getenv('TIDB_HOST'),
    user=os.getenv('TIDB_USER'),
    password=os.getenv('TIDB_PASSWORD'),
    database=os.getenv('TIDB_DATABASE'),
    port=4000,
    ssl_verify_cert=True
)

cursor = conn.cursor()
faker = Faker()

### 1. UPDATE: Modify behavior of existing customers
cursor.execute("SELECT customerID FROM churn_pipeline")
customer_ids = [row[0] for row in cursor.fetchall()]
sample_update = random.sample(customer_ids, k=min(30, len(customer_ids)))

for cid in sample_update:
    new_churn = random.choices(["Yes", "No"], weights=[0.3, 0.7])[0]
    new_tenure = random.randint(1, 72)
    new_charges = round(random.uniform(20, 120), 2)
    cursor.execute("""
        UPDATE churn_pipeline
        SET Churn=%s, tenure=%s, MonthlyCharges=%s
        WHERE customerID=%s
    """, (new_churn, new_tenure, new_charges, cid))

### 2. INSERT: Add new customers
contracts = ["Month-to-month", "One year", "Two year"]
for _ in range(10):  # Add 10 new customers
    customerID = faker.unique.uuid4()
    gender = random.choice(["Male", "Female"])
    SeniorCitizen = random.randint(0, 1)
    Partner = random.choice(["Yes", "No"])
    Dependents = random.choice(["Yes", "No"])
    tenure = random.randint(1, 72)
    MonthlyCharges = round(random.uniform(20, 120), 2)
    TotalCharges = round(MonthlyCharges * tenure, 2)
    Churn = random.choices(["Yes", "No"], weights=[0.2, 0.8])[0]
    Contract = random.choice(contracts)
    OnlineSecurity = random.choice(["Yes", "No", "No internet service"])
    TechSupport = random.choice(["Yes", "No", "No internet service"])

    cursor.execute("""
        INSERT INTO churn_pipeline (
            customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
            MonthlyCharges, TotalCharges, Churn, Contract, OnlineSecurity, TechSupport
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
        MonthlyCharges, TotalCharges, Churn, Contract, OnlineSecurity, TechSupport
    ))

### 3. DELETE (Optional): Simulate customers leaving the system entirely
sample_delete = random.sample(customer_ids, k=min(5, len(customer_ids)))
for cid in sample_delete:
    cursor.execute("DELETE FROM churn_pipeline WHERE customerID = %s", (cid,))

# Commit changes and close
conn.commit()
cursor.close()
conn.close()

print("✅ Churn pipeline table updated: 30 updates, 10 inserts, 5 deletions.")
