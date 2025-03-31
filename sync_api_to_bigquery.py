import requests
import pandas as pd
from google.cloud import bigquery

API_ENDPOINT = "https://api-gateway-eu.phorest.com/third-party-api-server/api/business/eDBVh3yNpsHz4UyirOti9A /branch/OW7zNFmHH1TK6G-F0LwPCA /appointment?fetch_canceled=true&fetch_deleted=true&fetch_archived=true&fetch_online_category=true&fetch_notes=true"
BEARER_TOKEN = "Z2xvYmFsL2FsaUBicmF1LmFlOkA5RGJGeXFDZmZEYQ=="  # Empty string if not provided
BIGQUERY_PROJECT = "brau-business-reports"
BIGQUERY_DATASET = "phorest_raw_staging"
BIGQUERY_TABLE = "appointments_test_centria"

def fetch_api_data(endpoint):
    headers = {
        "accept": "*/*",
        "authorization": f"Bearer {BEARER_TOKEN}" if BEARER_TOKEN else None
    }
    # Remove None values from headers
    headers = {k: v for k, v in headers.items() if v is not None}
    response = requests.get(endpoint, headers=headers)
    response.raise_for_status()
    return response.json()

def sync_to_bigquery(data, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    df = pd.DataFrame(data)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Synced {len(data)} records to {table_ref}")

if __name__ == "__main__":
    data = fetch_api_data(API_ENDPOINT)
    sync_to_bigquery(data, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
