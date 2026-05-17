# Fabric Notebook: Ingest MLB Final Game Snapshots
# Workspace: MLB-Bronze
# Lakehouse: mlb_raw_playbyplay
# Purpose: Detect newly-Final games and capture one canonical full play-by-play snapshot per game

import requests
import json
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

MLB_API_BASE = "https://statsapi.mlb.com/api"
FINAL_STATES = {"Final", "Game Over", "Completed Early"}

try:
    game_date = spark.conf.get("game_date")
except:
    game_date = date.today().strftime("%Y-%m-%d")

print(f"⚾ MLB Final Snapshot Capture - {game_date}")
print("=" * 60)

lifecycle_schema = StructType([
    StructField("game_pk", IntegerType(), False),
    StructField("game_date", StringType(), False),
    StructField("last_seen_status", StringType(), True),
    StructField("final_snapshot_captured", BooleanType(), False),
    StructField("final_snapshot_time", TimestampType(), True),
    StructField("final_snapshot_path", StringType(), True),
    StructField("last_updated", TimestampType(), False)
])

# ========================================
# Step 1: Find games needing finalization
# ========================================
print("\n🔍 Checking game lifecycle state...")

already_captured = set()
try:
    lifecycle_df = spark.table("game_lifecycle_state")

    # Games already captured — never re-fetch
    already_captured = {
        row.game_pk for row in lifecycle_df
        .filter(
            (col("game_date") == game_date) &
            (col("final_snapshot_captured") == True)
        ).select("game_pk").collect()
    }
    print(f"✅ Lifecycle table found: {len(already_captured)} games already have final snapshots")
except Exception as e:
    print(f"⚠️  game_lifecycle_state not yet available: {e}")
    print("ℹ️  Will rely on schedule API scan for Final games")

# Scan schedule API — single source of truth for game status
schedule_url = f"{MLB_API_BASE}/v1/schedule?sportId=1&date={game_date}"
try:
    resp = requests.get(schedule_url, timeout=30)
    resp.raise_for_status()
    all_games = []
    for date_entry in resp.json().get("dates", []):
        all_games.extend(date_entry.get("games", []))
    print(f"✅ Schedule: {len(all_games)} games on {game_date}")
except Exception as e:
    print(f"❌ Error fetching schedule: {e}")
    raise

games_to_finalize = [
    g for g in all_games
    if g.get("status", {}).get("abstractGameState") in FINAL_STATES
    and g["gamePk"] not in already_captured
]

print(f"✅ Games to finalize: {len(games_to_finalize)}")

if not games_to_finalize:
    print("ℹ️  No games require final snapshot - exiting")
    mssparkutils.notebook.exit(json.dumps({
        "status": "no_games_to_finalize",
        "game_date": game_date,
        "snapshots_captured": 0
    }))

# ========================================
# Step 2: Capture full canonical snapshot per game
# ========================================
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
captured = []
lifecycle_updates = []

for game in games_to_finalize:
    game_pk = game["gamePk"]
    home_team = game["teams"]["home"]["team"]["name"]
    away_team = game["teams"]["away"]["team"]["name"]
    game_status = game.get("status", {}).get("abstractGameState", "Unknown")

    print(f"\n📸 Capturing: {away_team} @ {home_team} (Game {game_pk})")

    game_url = f"{MLB_API_BASE}/v1.1/game/{game_pk}/feed/live"
    try:
        resp = requests.get(game_url, timeout=30)
        resp.raise_for_status()
        game_data = resp.json()

        all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])
        total_plays = len(all_plays)

        # Annotate with immutable snapshot metadata — never overwrite this file
        game_data["_snapshot_metadata"] = {
            "is_final_snapshot": True,
            "incremental": False,
            "capture_time": timestamp,
            "game_status_at_capture": game_status,
            "total_plays": total_plays
        }

        # Immutable file path — final_{timestamp} is never overwritten
        file_path = f"Files/raw/{game_date}/{game_pk}/final_{timestamp}.json"
        mssparkutils.fs.mkdirs(f"Files/raw/{game_date}/{game_pk}")
        mssparkutils.fs.put(file_path, json.dumps(game_data, indent=2), True)

        print(f"   ✅ Saved: {file_path} ({total_plays} plays)")

        captured.append({"game_pk": game_pk, "file_path": file_path, "total_plays": total_plays})
        lifecycle_updates.append({
            "game_pk": game_pk,
            "game_date": game_date,
            "last_seen_status": game_status,
            "final_snapshot_captured": True,
            "final_snapshot_time": datetime.utcnow(),
            "final_snapshot_path": file_path,
            "last_updated": datetime.utcnow()
        })

    except Exception as e:
        print(f"   ❌ Error capturing game {game_pk}: {e}")
        continue

# ========================================
# Step 3: Mark captured games in lifecycle table
# ========================================
if lifecycle_updates:
    print(f"\n💾 Marking {len(lifecycle_updates)} games as finalized...")

    update_df = spark.createDataFrame(lifecycle_updates, schema=lifecycle_schema)

    try:
        lifecycle_table = DeltaTable.forName(spark, "game_lifecycle_state")
        lifecycle_table.alias("target").merge(
            update_df.alias("source"),
            "target.game_pk = source.game_pk"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        print(f"   ✅ Lifecycle state updated for {len(lifecycle_updates)} games")
    except:
        update_df.write.format("delta").mode("overwrite").saveAsTable("game_lifecycle_state")
        print(f"   📝 Created lifecycle table with {len(lifecycle_updates)} games")

# ========================================
# Summary
# ========================================
print("\n" + "=" * 60)
print(f"✅ Final snapshot capture complete!")
print(f"   Game date:          {game_date}")
print(f"   Snapshots captured: {len(captured)}")
for c in captured:
    print(f"   Game {c['game_pk']}: {c['total_plays']} plays → {c['file_path']}")
print("=" * 60)

output = {
    "status": "success",
    "game_date": game_date,
    "snapshots_captured": len(captured),
    "games": captured
}

mssparkutils.notebook.exit(json.dumps(output))
