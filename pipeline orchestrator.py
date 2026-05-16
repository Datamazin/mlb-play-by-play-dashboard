"""
MLB Play-by-Play Data Pipeline Orchestrator
Polls MLB Stats API every 5 minutes, lands data in Azure Blob Storage,
then upserts into Azure SQL Database.
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
import requests
from azure.storage.blob import BlobServiceClient
import pyodbc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config (override via environment variables) ──────────────────────────────
MLB_BASE_URL   = "https://statsapi.mlb.com/api/v1"
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))   # 5 min
BLOB_CONN_STR  = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER_NAME", "mlb-raw")
SQL_CONN_STR   = os.getenv("AZURE_SQL_CONNECTION_STRING", "")


# ── MLB Stats API helpers ────────────────────────────────────────────────────

def get_todays_games() -> list[dict]:
    """Return all MLB games scheduled for today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{MLB_BASE_URL}/schedule?sportId=1&date={today}&hydrate=linescore"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    games = []
    for date_entry in data.get("dates", []):
        games.extend(date_entry.get("games", []))
    log.info("Found %d game(s) today (%s)", len(games), today)
    return games


def get_live_feed(game_pk: int) -> dict:
    """Pull the full live feed (play-by-play) for a single game."""
    url = f"{MLB_BASE_URL}.1/game/{game_pk}/feed/live"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def extract_plays(live_feed: dict) -> list[dict]:
    """Flatten allPlays from the live feed into a list of play records."""
    game_pk   = live_feed.get("gamePk")
    game_date = live_feed.get("gameData", {}).get("datetime", {}).get("officialDate")
    plays_raw = (
        live_feed
        .get("liveData", {})
        .get("plays", {})
        .get("allPlays", [])
    )
    plays = []
    for p in plays_raw:
        about   = p.get("about", {})
        result  = p.get("result", {})
        matchup = p.get("matchup", {})
        plays.append({
            "game_pk":          game_pk,
            "game_date":        game_date,
            "at_bat_index":     about.get("atBatIndex"),
            "inning":           about.get("inning"),
            "half_inning":      about.get("halfInning"),
            "is_complete":      about.get("isComplete"),
            "event":            result.get("event"),
            "event_type":       result.get("eventType"),
            "description":      result.get("description"),
            "rbi":              result.get("rbi"),
            "away_score":       result.get("awayScore"),
            "home_score":       result.get("homeScore"),
            "batter_id":        matchup.get("batter", {}).get("id"),
            "batter_name":      matchup.get("batter", {}).get("fullName"),
            "pitcher_id":       matchup.get("pitcher", {}).get("id"),
            "pitcher_name":     matchup.get("pitcher", {}).get("fullName"),
            "ingested_at_utc":  datetime.now(timezone.utc).isoformat(),
        })
    return plays


# ── Azure Blob Storage ───────────────────────────────────────────────────────

def upload_to_blob(game_pk: int, payload: dict) -> str:
    """
    Upload raw JSON live feed to Blob Storage.
    Path: mlb-raw/YYYY/MM/DD/<game_pk>/<timestamp>.json
    Returns the blob name.
    """
    if not BLOB_CONN_STR:
        log.warning("AZURE_STORAGE_CONNECTION_STRING not set – skipping blob upload")
        return ""

    now       = datetime.now(timezone.utc)
    blob_name = (
        f"{now.strftime('%Y/%m/%d')}"
        f"/{game_pk}"
        f"/{now.strftime('%H%M%S')}.json"
    )
    client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container = client.get_container_client(BLOB_CONTAINER)

    # Create container if it doesn't exist
    try:
        container.create_container()
    except Exception:
        pass  # already exists

    container.upload_blob(
        name=blob_name,
        data=json.dumps(payload, indent=2),
        overwrite=True
    )
    log.info("Uploaded raw feed → blob: %s", blob_name)
    return blob_name


# ── Azure SQL Database ───────────────────────────────────────────────────────

UPSERT_SQL = """
MERGE dbo.mlb_plays AS target
USING (VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)) AS source (
    game_pk, game_date, at_bat_index, inning, half_inning,
    is_complete, event, event_type, description, rbi,
    away_score, home_score, batter_id, batter_name,
    pitcher_id, pitcher_name
)
ON  target.game_pk      = source.game_pk
AND target.at_bat_index = source.at_bat_index
WHEN MATCHED THEN UPDATE SET
    is_complete  = source.is_complete,
    event        = source.event,
    event_type   = source.event_type,
    description  = source.description,
    rbi          = source.rbi,
    away_score   = source.away_score,
    home_score   = source.home_score,
    batter_id    = source.batter_id,
    batter_name  = source.batter_name,
    pitcher_id   = source.pitcher_id,
    pitcher_name = source.pitcher_name
WHEN NOT MATCHED THEN INSERT (
    game_pk, game_date, at_bat_index, inning, half_inning,
    is_complete, event, event_type, description, rbi,
    away_score, home_score, batter_id, batter_name,
    pitcher_id, pitcher_name
) VALUES (
    source.game_pk, source.game_date, source.at_bat_index,
    source.inning, source.half_inning, source.is_complete,
    source.event, source.event_type, source.description,
    source.rbi, source.away_score, source.home_score,
    source.batter_id, source.batter_name,
    source.pitcher_id, source.pitcher_name
);
"""


def upsert_plays(plays: list[dict]) -> int:
    """Upsert play records into Azure SQL. Returns number of rows affected."""
    if not SQL_CONN_STR:
        log.warning("AZURE_SQL_CONNECTION_STRING not set – skipping SQL upsert")
        return 0
    if not plays:
        return 0

    conn   = pyodbc.connect(SQL_CONN_STR, autocommit=False)
    cursor = conn.cursor()
    rows   = 0
    try:
        for play in plays:
            cursor.execute(UPSERT_SQL, (
                play["game_pk"],     play["game_date"],   play["at_bat_index"],
                play["inning"],      play["half_inning"], play["is_complete"],
                play["event"],       play["event_type"],  play["description"],
                play["rbi"],         play["away_score"],  play["home_score"],
                play["batter_id"],   play["batter_name"],
                play["pitcher_id"],  play["pitcher_name"],
            ))
            rows += cursor.rowcount
        conn.commit()
        log.info("Upserted %d play record(s) into SQL", rows)
    except Exception as exc:
        conn.rollback()
        log.error("SQL upsert failed: %s", exc)
        raise
    finally:
        cursor.close()
        conn.close()
    return rows


# ── Main loop ────────────────────────────────────────────────────────────────

def run_once():
    """Single pipeline execution: fetch → blob → SQL."""
    games = get_todays_games()
    active_statuses = {"In Progress", "Warmup", "Pre-Game"}

    for game in games:
        status  = game.get("status", {}).get("detailedState", "")
        game_pk = game["gamePk"]

        if status not in active_statuses:
            log.debug("Skipping game %s (status: %s)", game_pk, status)
            continue

        log.info("Processing game %s  [%s]", game_pk, status)
        try:
            feed  = get_live_feed(game_pk)
            plays = extract_plays(feed)

            upload_to_blob(game_pk, feed)
            upsert_plays(plays)

            log.info("Game %s → %d plays processed", game_pk, len(plays))
        except Exception as exc:
            log.error("Error on game %s: %s", game_pk, exc)


def main():
    log.info("MLB pipeline started – polling every %ds", POLL_INTERVAL)
    while True:
        start = time.monotonic()
        try:
            run_once()
        except Exception as exc:
            log.error("Pipeline run failed: %s", exc)
        elapsed = time.monotonic() - start
        sleep_for = max(0, POLL_INTERVAL - elapsed)
        log.info("Sleeping %.1fs until next poll", sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
