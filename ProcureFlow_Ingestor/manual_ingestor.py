import boto3
import pandas as pd
import io
import os

# --- CONFIGURATION ---
BUCKET_NAME = "procureflow-datalake-raw"
LANDING_PREFIX = "landing_zone/"
RAW_PREFIX = "raw_data/"

# Define Schema (Crucial for .dat files)
COLUMN_NAMES = [
    'transaction_id', 'customer_id', 'customer_name', 'customer_email',
    'customer_age', 'store_location', 'product_category', 'unit_price',
    'quantity', 'discount_rate', 'tax_amount', 'total_amount',
    'transaction_date', 'payment_status', 'return_flag'
]

s3 = boto3.client('s3')

def run_ingestion():
    print(f"Scanning bucket '{BUCKET_NAME}' for legacy files...")
    
    # 1. List all files in Landing Zone
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=LANDING_PREFIX)
    
    if 'Contents' not in response:
        print("No files found in landing_zone/. Waiting for next batch...")
        return

    files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.dat')]
    print(f"Found {len(files)} .dat files to process.\n")
    
    # 2. Loop through and process
    for i, file_key in enumerate(files, 1):
        filename = file_key.split('/')[-1]
        print(f" [{i}/{len(files)}] Processing {filename}...")
        
        try:
            # A. READ (Download & Read as String)
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            
            # dtype=str prevents conversion errors at this stage
            df = pd.read_csv(
                io.BytesIO(obj['Body'].read()), 
                sep='|', 
                names=COLUMN_NAMES,
                header=None,
                dtype=str 
            )
            
            # B. CONVERT (To Parquet)
            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=False)
            
            # C. UPLOAD (To Raw Data)
            new_key = file_key.replace(LANDING_PREFIX, RAW_PREFIX).replace(".dat", ".parquet")
            s3.put_object(Bucket=BUCKET_NAME, Key=new_key, Body=out_buffer.getvalue())
            print(f" Saved to: {new_key}")

            # --- D. DELETE ORIGINAL (Critical for Nightly Loop) ---
            s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
            print(f" Deleted original: {file_key}")
            
        except Exception as e:
            print(f" Failed to process {filename}: {e}")

    print("\nIngestion Batch Complete!")

if __name__ == "__main__":
    run_ingestion()