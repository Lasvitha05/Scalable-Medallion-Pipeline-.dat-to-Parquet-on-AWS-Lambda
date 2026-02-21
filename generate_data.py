import boto3
import pandas as pd
import numpy as np
import io
import time
import random
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BUCKET_NAME = "procureflow-datalake-raw"
LANDING_ZONE_PREFIX = "landing_zone/"
NUM_FILES = 10        # Number of files per batch
ROWS_PER_FILE = 800000 # Number of rows per file

s3 = boto3.client('s3')

def generate_data():
    print(f"Generating {NUM_FILES} legacy .dat files...")
    
    for i in range(NUM_FILES):
        # 1. Create Random Data
        data = {
            'transaction_id': [f"TXN-{random.randint(10000, 99999)}" for _ in range(ROWS_PER_FILE)],
            'customer_id': [f"CUST-{random.randint(1000, 9999)}" for _ in range(ROWS_PER_FILE)],
            'customer_name': [f"Customer_{random.randint(1, 100)}" for _ in range(ROWS_PER_FILE)],
            'customer_email': [f"user{random.randint(1, 100)}@example.com" for _ in range(ROWS_PER_FILE)],
            'customer_age': np.random.randint(18, 90, ROWS_PER_FILE),
            'store_location': np.random.choice(['New York', 'London', 'Tokyo', 'Paris'], ROWS_PER_FILE),
            'product_category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Books'], ROWS_PER_FILE),
            'unit_price': np.round(np.random.uniform(10, 500, ROWS_PER_FILE), 2),
            'quantity': np.random.randint(1, 10, ROWS_PER_FILE),
            'discount_rate': np.round(np.random.uniform(0, 0.3), 2),
            'tax_amount': np.round(np.random.uniform(1, 50), 2),
            'total_amount': np.round(np.random.uniform(10, 1000), 2),
            'transaction_date': [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(ROWS_PER_FILE)],
            'payment_status': np.random.choice(['Paid', 'Pending', 'Failed'], ROWS_PER_FILE),
            'return_flag': np.random.choice(['Yes', 'No'], ROWS_PER_FILE)
        }
        
        df = pd.DataFrame(data)
        
        # 2. Convert to Pipe-Delimited String (Legacy Format)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep='|', index=False, header=False)
        
        # 3. Upload to S3
        file_name = f"batch_{int(time.time())}_{i}.dat"
        s3_key = f"{LANDING_ZONE_PREFIX}{file_name}"
        
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        print(f" Uploaded: {s3_key}")

if __name__ == "__main__":
    generate_data()