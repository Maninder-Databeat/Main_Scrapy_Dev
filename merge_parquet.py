import os
import pandas as pd
import pyarrow.parquet as pq

def merge_parquet_files(folder_path, output_file):
    """
    Combines all Parquet files in the given folder into a single Parquet file.

    Parameters:
        folder_path (str): Path to the folder containing Parquet files.
        output_file (str): Path for the merged Parquet file.

    Returns:
        None
    """
    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]

    if not parquet_files:
        print("No Parquet files found in the folder.")
        return

    dataframes = [pd.read_parquet(file) for file in parquet_files]
    
    merged_df = pd.concat(dataframes, ignore_index=True)

    merged_df['inventory_type'] = 'ads.txt'
    merged_df['http_client'] = 'curl_cffi'
    merged_df['execution_date'] = '2025-03-16T18:47:38.890864+00:00'

    # Save the merged DataFrame as a new Parquet file
    merged_df.to_parquet(output_file, engine='pyarrow', index=False)
    
    print(f"Merged {len(parquet_files)} Parquet files into '{output_file}'.")

# Example Usage:
folder_path = r"previous_run\ads_txt_metadata\execution_date=2025-03-14T11%3A28%3A49.256720%2B00%3A00"  # Change this to your folder path
output_file = "merged_metadata.parquet"  # Change this to your desired output file
merge_parquet_files(folder_path, output_file)
