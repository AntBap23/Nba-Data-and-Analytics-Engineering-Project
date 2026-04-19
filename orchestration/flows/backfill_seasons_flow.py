import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def run_step(step_name: str, command: list[str]) -> None:
    """Run one pipeline step and fail fast with a clear message."""
    print(f"\n[{step_name}] Running: {' '.join(command)}")
    subprocess.run(command, check=True, cwd=REPO_ROOT)
    print(f"[{step_name}] Completed successfully")


def main() -> None:
    run_step(
        "fetch",
        [sys.executable, "scripts/fetch_ten_seasons.py", "--output-dir", "data/temp"],
    )
    run_step("upload", [sys.executable, "ingestion/upload_to_gcs.py"])
    run_step("load", [sys.executable, "ingestion/load_to_bigquery.py"])


if __name__ == "__main__":
    main()
