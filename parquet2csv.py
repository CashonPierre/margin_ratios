
## convert parquet to csv
import pandas as pd
import os
import pyarrow.parquet as pq

def convert_parquet_to_csv(parquet_file, csv_file):
    try:
        # Read the parquet file
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        # Save to CSV
        df.to_csv(csv_file, index=False)
        print(f"Successfully converted {parquet_file} to {csv_file}")
    except Exception as e:
        print(f"Error converting {parquet_file} to CSV: {e}")

if __name__ == "__main__":
    parquet_file = "futu_margin_snapshot_0317.parquet"  # Replace with your parquet file path
    csv_file = "output_data.csv"          # Desired output CSV file path
    convert_parquet_to_csv(parquet_file, csv_file)