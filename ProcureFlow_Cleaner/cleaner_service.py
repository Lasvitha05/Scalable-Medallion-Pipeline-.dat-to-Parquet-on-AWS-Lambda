import boto3
import pandas as pd
import io
import urllib.parse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("CLEANER SERVICE STARTED")
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        raw_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        if "raw_data/" not in raw_key:
            continue
            
        print(f"   Processing Raw File: {raw_key}")
        
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=raw_key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            
            # --- 1. CLEAN TEXT COLUMNS (Strip whitespace, handle NaN) ---
            text_cols = [
                'transaction_id', 'customer_id', 'customer_name', 
                'customer_email', 'store_location', 'product_category', 
                'payment_status'
            ]
            
            for col in text_cols:
                # Convert to string, strip whitespace, replace "nan" strings with None
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({'nan': None, 'None': None, '': None})

            # --- 2. CLEAN NUMERIC COLUMNS (Float) ---
            float_cols = ['total_amount', 'unit_price', 'discount_rate', 'tax_amount']
            for col in float_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # --- 3. CLEAN INTEGER COLUMNS ---
            # Fill NaN with 0 (or -1 if you prefer) to safely cast to Int
            int_cols = ['quantity', 'customer_age']
            for col in int_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

            # --- 4. CLEAN DATE COLUMN ---
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

            # --- 5. CLEAN BOOLEAN COLUMN ---
            true_values = ['yes', 'y', 'true', '1']
            # Ensure we are checking a string version of the data to avoid errors
            df['return_flag'] = df['return_flag'].astype(str).str.lower().isin(true_values)

            # --- WRITE TO S3 ---
            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=False)
            
            clean_key = raw_key.replace("raw_data/", "clean_data/")
            s3.put_object(Bucket=bucket_name, Key=clean_key, Body=out_buffer.getvalue())
            print(f"  Fully Cleaned & Saved to: {clean_key}")
            
        except Exception as e:
            print(f"Error cleaning {raw_key}: {str(e)}")
            raise e
            
    return {"status": "Cleaning Complete"}
