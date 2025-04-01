
from pathlib import Path
import shutil

from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from google.cloud import storage
import numpy as np
import pandas as pd
import gspread

class GoogleServices:

    SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
    ,"https://www.googleapis.com/auth/devstorage.full_control"
]

    def __init__(self, project_id:str, json_credentials_path:str):
        self.project_id = project_id
        self.json_credentials_path = json_credentials_path

        credentials = Credentials.from_service_account_file(self.json_credentials_path, scopes=GoogleServices.SCOPES)

        self.bq_client:bigquery.Client = bigquery.Client(project=project_id, credentials=credentials)
        print(self.bq_client.project)
        self.storage_client:storage.Client = storage.Client(project=project_id, credentials=credentials)
        self.sheet_client:gspread.Client = gspread.Client(auth=credentials)

    def adhoc_gsheet_pull(self):

        google_sheet_id = "1DLevo_k0LTfIHH7wL9n08NfYnpq_CbSF5RLNIMaIJU8"
        worksheet_name = "main_file"

        spreadsheet = self.sheet_client.open_by_key(google_sheet_id)

        worksheet = spreadsheet.worksheet(worksheet_name)

        list_of_dicts = worksheet.get_all_records()

        df = pd.DataFrame(list_of_dicts)

        df.rename(
            columns={
                "Publisher" : "publisher",
                "ORG_ID" : "org_id",
                "Inventory_Type" : "inventory_type",
                "Relationship_Type" : "relationship_type",
                "Account_Manager" : "account_manager",
                "Account_Manager_Email" : "account_manager_email",
                "Domain" : "domain",
                "Ad_Request" : "ad_request",
                "Revenue" : "revenue",
            }
            , inplace=True
        )

        df['inventory_type'] = np.where(
            df['inventory_type'].str.contains('app-ads.txt', case=False, na=False),
            'app-ads.txt',
            'ads.txt'
            )

        df["ads_page_url"] = ""
        df["http_client"] = "curl_cffi"

        return df

    def replace_gcs_parquet_to_bq(self, gcs_uri, dataset_id, table_id):
        """
        Append Parquet file data from GCS to an existing BigQuery table.

        Parameters:
            gcs_uri (str): Path to the Parquet file(s) in GCS (e.g., "gs://my-bucket/my-folder/*.parquet").
            dataset_id (str): BigQuery dataset ID.
            table_id (str): BigQuery table ID.
            project_id (str): Google Cloud project ID.
        """

        client:bigquery.Client = self.bq_client
        table_ref = client.dataset(dataset_id, self.project_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Append Mode
        )

        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )

        load_job.result()  # Wait for the job to complete
        print(f"Replaced data from {gcs_uri} to {dataset_id}.{table_id}")

    # # Example Usage
    # append_parquet_to_bq(
    #     gcs_uri="gs://your-bucket-name/path-to-files/*.parquet",
    #     dataset_id="your_dataset",
    #     table_id="your_table",
    #     project_id="your_project_id"
    # )

    def query_bq_to_pd(self, sql_query: str) -> pd.DataFrame:
        """
        Queries Google BigQuery and returns the result as a Pandas DataFrame.

        :param sql_query: The SQL query string to execute in BigQuery.
        :param project_id: The GCP project ID where BigQuery is enabled.
        :return: Pandas DataFrame with query results.
        """
        client = self.bq_client
        query_job = client.query(sql_query)
        df = query_job.result().to_dataframe()
        return df

    def upload_parquet_to_gcs(self, local_file_path, bucket_name, destination_blob_name):
        """
        Uploads a Parquet file to Google Cloud Storage (GCS).

        Parameters:
            local_file_path (str): Path to the local Parquet file.
            bucket_name (str): Name of the GCS bucket.
            destination_blob_name (str): Destination path in GCS (e.g., "folder/filename.parquet").
        """

        # Initialize a GCS client
        client:storage.Client = self.storage_client

        if Path(local_file_path).exists():

            # Get the bucket
            bucket = client.bucket(bucket_name)

            # Define the blob (file) in GCS
            blob = bucket.blob(destination_blob_name)

            # Upload the file
            blob.upload_from_filename(local_file_path)

            print(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")

            return f"gs://{bucket_name}/{destination_blob_name}"
        
        return None

    # # Example Usage:
    # upload_parquet_to_gcs(
    #     local_file_path="data/sample.parquet",  # Local Parquet file path
    #     bucket_name="your-bucket-name",  # GCS Bucket Name
    #     destination_blob_name="parquet_files/sample.parquet"  # GCS Destination Path
    # )
    def service_close(self):
        self.bq_client.close()
        self.storage_client.close()

        return None

def remove_dir(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f"Folder '{folder_path}' and its contents deleted successfully.")
    except FileNotFoundError:
        print(f"Folder '{folder_path}' not found.")
    except OSError as e:
        print(f"Error deleting folder '{folder_path}': {e}")

if __name__ == "__main__":

    # try:
    #     gservice = GoogleServices('ads-txt-validator', r".secrets/ads-txt-validator-BQ.json")

    #     storage_uri = gservice.upload_parquet_to_gcs(r"data_output\2025-03-19\ads_txt_metadata_2025-03-19-07-38-33.parquet","ads_txy_bucket_db","bronze/ads_txt_metadata/metadata.parquet")

    #     gservice.replace_gcs_parquet_to_bq(storage_uri, dataset_id="ads_txt_scraper_data", table_id="domain_meta_data")
    
    # finally:
    #     gservice.service_close()

    # remove_dir(r"data_output/2025-03-16")
    pass