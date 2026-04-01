import argparse
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests


NBA_STATS_URL = "https://stats.nba.com/stats/leaguegamefinder"
OUTPUT_DIR = Path("data")
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CALLS = 1.25
DEFAULT_SEASON_COUNT = 10
WINDOW_DAYS = 14
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 5

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://www.nba.com",
    "Pragma": "no-cache",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
}


def season_label(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def last_completed_season_start(current_year: int, current_month: int) -> int:
    # NBA regular seasons typically end around April-June. Before July, treat the
    # previous season as the last completed one.
    if current_month < 7:
        return current_year - 2
    return current_year - 1


def build_recent_seasons(season_count: int) -> list[str]:
    now = pd.Timestamp.now(tz="America/Chicago")
    end_start_year = last_completed_season_start(now.year, now.month)
    start_start_year = end_start_year - season_count + 1
    return [season_label(year) for year in range(start_start_year, end_start_year + 1)]


def build_date_windows(season: str, window_days: int = WINDOW_DAYS) -> list[tuple[str, str]]:
    start_year = int(season.split("-")[0])
    season_start = pd.Timestamp(start_year, 10, 1)
    season_end = pd.Timestamp(start_year + 1, 6, 30)

    windows: list[tuple[str, str]] = []
    current_start = season_start
    while current_start <= season_end:
        current_end = min(current_start + timedelta(days=window_days - 1), season_end)
        windows.append(
            (
                current_start.strftime("%m/%d/%Y"),
                current_end.strftime("%m/%d/%Y"),
            )
        )
        current_start = current_end + timedelta(days=1)
    return windows


def fetch_window_logs(
    session: requests.Session,
    season: str,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    params = {
        "Conference": "",
        "DateFrom": date_from,
        "DateTo": date_to,
        "Division": "",
        "DraftNumber": "",
        "DraftRound": "",
        "DraftTeamID": "",
        "DraftYear": "",
        "EqAST": "",
        "EqBLK": "",
        "EqDD": "",
        "EqDREB": "",
        "EqFG3A": "",
        "EqFG3M": "",
        "EqFG3_PCT": "",
        "EqFGA": "",
        "EqFGM": "",
        "EqFG_PCT": "",
        "EqFTA": "",
        "EqFTM": "",
        "EqFT_PCT": "",
        "EqMINUTES": "",
        "EqOREB": "",
        "EqPF": "",
        "EqPTS": "",
        "EqREB": "",
        "EqSTL": "",
        "EqTD": "",
        "EqTOV": "",
        "GameID": "",
        "GtAST": "",
        "GtBLK": "",
        "GtDD": "",
        "GtDREB": "",
        "GtFG3A": "",
        "GtFG3M": "",
        "GtFG3_PCT": "",
        "GtFGA": "",
        "GtFGM": "",
        "GtFG_PCT": "",
        "GtFTA": "",
        "GtFTM": "",
        "GtFT_PCT": "",
        "GtMINUTES": "",
        "GtOREB": "",
        "GtPF": "",
        "GtPTS": "",
        "GtREB": "",
        "GtSTL": "",
        "GtTD": "",
        "GtTOV": "",
        "LeagueID": "00",
        "Location": "",
        "LtAST": "",
        "LtBLK": "",
        "LtDD": "",
        "LtDREB": "",
        "LtFG3A": "",
        "LtFG3M": "",
        "LtFG3_PCT": "",
        "LtFGA": "",
        "LtFGM": "",
        "LtFG_PCT": "",
        "LtFTA": "",
        "LtFTM": "",
        "LtFT_PCT": "",
        "LtMINUTES": "",
        "LtOREB": "",
        "LtPF": "",
        "LtPTS": "",
        "LtREB": "",
        "LtSTL": "",
        "LtTD": "",
        "LtTOV": "",
        "Outcome": "",
        "PORound": "0",
        "PlayerOrTeam": "P",
        "RookieYear": "",
        "Season": "",
        "SeasonSegment": "",
        "SeasonType": "Regular Season",
        "SeasonTypeNullable": "Regular Season",
        "SeasonNullable": season,
        "StarterBench": "",
        "TeamID": "",
        "VsConference": "",
        "VsDivision": "",
        "VsTeamID": "",
    }
    last_error: requests.exceptions.RequestException | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(NBA_STATS_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            result_set = payload["resultSets"][0]
            frame = pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])
            return frame
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            print(
                f"    Retry {attempt}/{MAX_RETRIES - 1} after error for "
                f"{season} {date_from} to {date_to}: {exc}"
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    assert last_error is not None
    raise last_error


def fetch_season_logs(session: requests.Session, season: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for date_from, date_to in build_date_windows(season):
        print(f"  Window {date_from} to {date_to}")
        frame = fetch_window_logs(session, season, date_from, date_to)
        if not frame.empty:
            frames.append(frame)
        time.sleep(SLEEP_BETWEEN_CALLS)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["GAME_ID", "PLAYER_ID"])
    combined["SEASON_LABEL"] = season
    return combined


def save_season_csv(frame: pd.DataFrame, season: str, output_dir: Path) -> Path:
    output_path = output_dir / f"player_logs_{season}.csv"
    frame.to_csv(output_path, index=False)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NBA regular-season player game logs and save one CSV per season."
    )
    parser.add_argument(
        "--seasons",
        type=int,
        default=DEFAULT_SEASON_COUNT,
        help="Number of completed seasons to fetch, counting backward from the latest completed NBA season.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Directory where season CSVs should be written.",
    )
    parser.add_argument(
        "--combined-name",
        default="",
        help="Optional filename for a combined CSV export. Leave unset to save yearly files only.",
    )
    parser.add_argument(
        "--season-labels",
        default="",
        help='Comma-separated season labels to fetch directly, for example "2019-20,2020-21".',
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip fetching seasons whose CSV already exists in the output directory.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.season_labels.strip():
        seasons = [label.strip() for label in args.season_labels.split(",") if label.strip()]
    else:
        seasons = build_recent_seasons(args.seasons)

    print(f"Fetching {len(seasons)} completed NBA seasons: {', '.join(seasons)}")

    session = requests.Session()
    session.headers.update(HEADERS)

    season_frames: list[pd.DataFrame] = []
    try:
        for season in seasons:
            season_output_path = output_dir / f"player_logs_{season}.csv"
            if args.skip_existing and season_output_path.exists():
                print(f"Skipping {season}; found existing export at {season_output_path}")
                existing_frame = pd.read_csv(season_output_path)
                season_frames.append(existing_frame)
                continue

            print(f"Requesting {season}...")
            frame = fetch_season_logs(session, season)
            csv_path = save_season_csv(frame, season, output_dir)
            season_frames.append(frame)
            print(f"Saved {len(frame):,} rows to {csv_path}")
    except requests.exceptions.RequestException as exc:
        raise SystemExit(
            "NBA data download failed. If you are running in a restricted sandbox, "
            "re-run with network access enabled.\n"
            f"Original error: {exc}"
        ) from exc

    if args.combined_name:
        combined = pd.concat(season_frames, ignore_index=True)
        combined_path = output_dir / args.combined_name
        combined.to_csv(combined_path, index=False)
        print(f"Saved combined export with {len(combined):,} rows to {combined_path}")


if __name__ == "__main__":
    main()
