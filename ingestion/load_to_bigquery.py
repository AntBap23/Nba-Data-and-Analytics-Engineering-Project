import os
import re

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage

load_dotenv()


DEFAULT_GCS_PREFIX = "raw/nba/player_logs"
FILE_PATTERN = re.compile(r"player_logs_(\d{4})-\d{2}\.csv$")


def get_required_env(var_name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def build_table_name(file_name: str) -> str:
    """Map a season file name to the raw table name expected by dbt."""
    match = FILE_PATTERN.fullmatch(file_name)
    if not match:
        raise ValueError(
            "Expected filenames like player_logs_2024-25.csv, "
            f"received: {file_name}"
        )

    season_start_year = match.group(1)
    return f"player_game_logs_{season_start_year}_raw"


def list_gcs_csv_uris(bucket_name: str, gcs_prefix: str) -> list[str]:
    """Return sorted CSV URIs from the configured raw GCS prefix."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=gcs_prefix)

    uris = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".csv") and not blob.name.endswith("/")
    ]
    return sorted(uris)


def load_uri_to_table(
    client: bigquery.Client,
    table_id: str,
    uri: str,
) -> int:
    """Load one season CSV from GCS into its matching BigQuery raw table."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Starting load job for {uri} -> {table_id}")
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows:,} rows into {table_id}")
    return destination_table.num_rows


def load_all_raw_files(
    project_id: str,
    dataset: str,
    bucket_name: str,
    gcs_prefix: str,
) -> None:
    """Load all season CSV files from GCS into BigQuery raw tables."""
    uris = list_gcs_csv_uris(bucket_name=bucket_name, gcs_prefix=gcs_prefix)
    if not uris:
        print(f"No CSV files found in gs://{bucket_name}/{gcs_prefix}")
        return

    client = bigquery.Client(project=project_id)
    loaded_tables = 0
    total_rows = 0

    for uri in uris:
        file_name = uri.rsplit("/", 1)[-1]
        table_name = build_table_name(file_name)
        table_id = f"{project_id}.{dataset}.{table_name}"

        row_count = load_uri_to_table(client=client, table_id=table_id, uri=uri)
        loaded_tables += 1
        total_rows += row_count

    print(
        f"Finished loading {loaded_tables} table(s) from "
        f"gs://{bucket_name}/{gcs_prefix} with {total_rows:,} total row(s)."
    )


def main() -> None:
    project_id = get_required_env("GCP_PROJECT_ID")
    dataset = get_required_env("BQ_DATASET")
    bucket_name = get_required_env("GCS_BUCKET")
    gcs_prefix = os.getenv("GCS_RAW_PREFIX", DEFAULT_GCS_PREFIX).rstrip("/")

    load_all_raw_files(
        project_id=project_id,
        dataset=dataset,
        bucket_name=bucket_name,
        gcs_prefix=gcs_prefix,
    )


if __name__ == "__main__":
    main()
