import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

DEFAULT_DATA_DIR = Path("data/temp")
DEFAULT_GCS_PREFIX = "raw/nba/player_logs"


def get_required_env(var_name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def upload_to_storage(bucket_name: str, data_dir: Path, gcs_prefix: str) -> None:
    """Upload all temp CSV files to GCS and delete each file after success."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    file_paths = sorted(data_dir.glob("*.csv"))

    if not file_paths:
        print(f"No CSV files found in {data_dir}")
        return

    uploaded_count = 0

    for file_path in file_paths:
        destination_path = f"{gcs_prefix}/{file_path.name}"
        blob = bucket.blob(destination_path)

        print(f"Uploading {file_path} to gs://{bucket_name}/{destination_path}")
        blob.upload_from_filename(str(file_path))
        file_path.unlink()

        uploaded_count += 1
        print(f"Uploaded and deleted local temp file: {file_path.name}")

    print(f"Uploaded {uploaded_count} file(s) to gs://{bucket_name}/{gcs_prefix}")


def main() -> None:
    bucket_name = get_required_env("GCS_BUCKET")
    data_dir = Path(os.getenv("TEMP_DATA_DIR", str(DEFAULT_DATA_DIR)))
    gcs_prefix = os.getenv("GCS_RAW_PREFIX", DEFAULT_GCS_PREFIX).rstrip("/")

    data_dir.mkdir(parents=True, exist_ok=True)
    upload_to_storage(bucket_name=bucket_name, data_dir=data_dir, gcs_prefix=gcs_prefix)


if __name__ == "__main__":
    main()


