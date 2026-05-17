# Fabric Notebook: Ingest MLB Play-by-Play Data (INCREMENTAL)
# Workspace: MLB-Bronze
# Lakehouse: mlb_raw_playbyplay
# Purpose: Fetch ONLY NEW plays since last run - reduces storage by 80-90%

import requests
import json
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
MLB_API_BASE = "https://statsapi.mlb.com/api"
TODAY = date.today().strftime("%Y-%m-%d")

print(f"⚾ MLB Incremental Ingestion - {TODAY}")
print("=" * 60)

# ========================================
# Step 1: Load Game Checkpoint State
# ========================================
print("\n📋 Loading checkpoint state...")

# Define checkpoint table schema
checkpoint_schema = StructType([
    StructField("game_pk", IntegerType(), False),
    StructField("game_date", StringType(), False),
    StructField("last_play_index", IntegerType(), False),
    StructField("last_at_bat_index", IntegerType(), False),
    StructField("total_plays_captured", IntegerType(), False),
    StructField("last_updated", TimestampType(), False),
    StructField("game_status", StringType(), True)
])

# Check if checkpoint table exists
checkpoint_table_name = "game_checkpoints"

try:
    checkpoint_df = spark.table(checkpoint_table_name)
    print(f"✅ Loaded existing checkpoints: {checkpoint_df.count()} games tracked")
    
    # Get checkpoint data as dictionary for quick lookup
    checkpoints = {
        row.game_pk: {
            'last_play_index': row.last_play_index,
            'last_at_bat_index': row.last_at_bat_index,
            'total_plays_captured': row.total_plays_captured,
            'game_status': row.game_status
        }
        for row in checkpoint_df.collect()
    }
except:
    print("📝 Checkpoint table doesn't exist - will create on first run")
    checkpoints = {}

# ========================================
# Step 2: Get Today's Active Games
# ========================================
schedule_url = f"{MLB_API_BASE}/v1/schedule?sportId=1&date={TODAY}"
print(f"\n📅 Fetching schedule: {schedule_url}")

try:
    schedule_response = requests.get(schedule_url, timeout=30)
    schedule_response.raise_for_status()
    schedule_data = schedule_response.json()
except Exception as e:
    print(f"❌ Error fetching schedule: {e}")
    raise

# Extract games
games = []
if 'dates' in schedule_data and len(schedule_data['dates']) > 0:
    for date_entry in schedule_data['dates']:
        if 'games' in date_entry:
            games.extend(date_entry['games'])

print(f"✅ Found {len(games)} games today")

# Filter active games
ACTIVE_STATES = {'Live', 'In Progress', 'Warmup', 'Pre-Game'}
active_games = [
    game for game in games 
    if game.get('status', {}).get('abstractGameState') in ACTIVE_STATES
]

print(f"🟢 {len(active_games)} active games")

if len(active_games) == 0:
    print("ℹ️  No active games - exiting")
    mssparkutils.notebook.exit(json.dumps({
        "status": "no_active_games",
        "total_games": len(games),
        "active_games": 0
    }))

# ========================================
# Step 3: Incremental Fetch & Store
# ========================================
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
files_saved = []
new_plays_total = 0
updated_checkpoints = []

for game in active_games:
    game_pk = game['gamePk']
    home_team = game['teams']['home']['team']['name']
    away_team = game['teams']['away']['team']['name']
    game_status = game.get('status', {}).get('abstractGameState', 'Unknown')
    
    print(f"\n📥 Processing: {away_team} @ {home_team} (Game {game_pk})")
    
    # Get checkpoint for this game
    checkpoint = checkpoints.get(game_pk, {
        'last_play_index': -1,
        'last_at_bat_index': -1,
        'total_plays_captured': 0,
        'game_status': 'New'
    })
    
    last_captured = checkpoint['total_plays_captured']
    print(f"   📊 Previous state: {last_captured} plays captured")
    
    # Fetch live game feed
    game_url = f"{MLB_API_BASE}/v1.1/game/{game_pk}/feed/live"
    
    try:
        game_response = requests.get(game_url, timeout=30)
        game_response.raise_for_status()
        game_data = game_response.json()
        
        # Extract all plays from current game state
        all_plays = game_data.get('liveData', {}).get('plays', {}).get('allPlays', [])
        total_plays_now = len(all_plays)
        
        # Determine if there are new plays
        if total_plays_now <= last_captured:
            print(f"   ⏭️  No new plays (still at {total_plays_now})")
            
            # Update checkpoint timestamp even if no new plays
            updated_checkpoints.append({
                'game_pk': game_pk,
                'game_date': game['gameDate'][:10],
                'last_play_index': checkpoint['last_play_index'],
                'last_at_bat_index': checkpoint['last_at_bat_index'],
                'total_plays_captured': last_captured,
                'last_updated': datetime.utcnow(),
                'game_status': game_status
            })
            continue
        
        # Extract ONLY NEW plays
        new_plays = all_plays[last_captured:]
        new_play_count = len(new_plays)
        new_plays_total += new_play_count
        
        print(f"   ✨ NEW PLAYS: {new_play_count} (from {last_captured} → {total_plays_now})")
        
        # Create incremental payload with ONLY new plays
        incremental_data = {
            'gameData': game_data.get('gameData', {}),
            'liveData': {
                'plays': {
                    'allPlays': new_plays,  # Only new plays!
                    'currentPlay': game_data.get('liveData', {}).get('plays', {}).get('currentPlay', {})
                }
            },
            'metadata': {
                'incremental': True,
                'play_range': f"{last_captured + 1}-{total_plays_now}",
                'capture_time': timestamp,
                'checkpoint_state': checkpoint
            }
        }
        
        # Storage path for incremental data
        game_date = game['gameDate'][:10]
        file_path = f"Files/raw/{game_date}/{game_pk}/incremental_{timestamp}.json"
        
        # Create directory structure
        dir_path = f"Files/raw/{game_date}/{game_pk}"
        mssparkutils.fs.mkdirs(dir_path)
        
        # Write incremental JSON
        json_content = json.dumps(incremental_data, indent=2)
        mssparkutils.fs.put(file_path, json_content, True)
        
        print(f"   ✅ Saved: {file_path} ({new_play_count} new plays)")
        
        # Update checkpoint
        last_play = new_plays[-1] if new_plays else None
        updated_checkpoints.append({
            'game_pk': game_pk,
            'game_date': game_date,
            'last_play_index': last_play.get('atBatIndex', checkpoint['last_play_index']) if last_play else checkpoint['last_play_index'],
            'last_at_bat_index': last_play.get('about', {}).get('atBatIndex', checkpoint['last_at_bat_index']) if last_play else checkpoint['last_at_bat_index'],
            'total_plays_captured': total_plays_now,
            'last_updated': datetime.utcnow(),
            'game_status': game_status
        })
        
        files_saved.append({
            'game_pk': game_pk,
            'file_path': file_path,
            'home_team': home_team,
            'away_team': away_team,
            'new_plays': new_play_count,
            'total_plays': total_plays_now,
            'timestamp': timestamp
        })
        
    except Exception as e:
        print(f"   ❌ Error fetching game {game_pk}: {e}")
        continue

# ========================================
# Step 4: Update Checkpoint Table
# ========================================
if updated_checkpoints:
    print(f"\n💾 Updating checkpoint table...")
    
    checkpoint_update_df = spark.createDataFrame(updated_checkpoints, schema=checkpoint_schema)
    
    try:
        # Try to MERGE with existing checkpoint table
        checkpoint_table = DeltaTable.forName(spark, checkpoint_table_name)
        
        checkpoint_table.alias("target").merge(
            checkpoint_update_df.alias("source"),
            "target.game_pk = source.game_pk"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        
        print(f"   ✅ Updated {len(updated_checkpoints)} game checkpoints")
        
    except:
        # Create checkpoint table if it doesn't exist
        print(f"   📝 Creating new checkpoint table")
        checkpoint_update_df.write.format("delta").mode("overwrite").saveAsTable(checkpoint_table_name)
        print(f"   ✅ Created checkpoint table with {len(updated_checkpoints)} games")

# ========================================
# Step 5: Update Game Lifecycle State
# ========================================
print("\n🔄 Updating game lifecycle state...")

lifecycle_schema = StructType([
    StructField("game_pk", IntegerType(), False),
    StructField("game_date", StringType(), False),
    StructField("last_seen_status", StringType(), True),
    StructField("final_snapshot_captured", BooleanType(), False),
    StructField("final_snapshot_time", TimestampType(), True),
    StructField("final_snapshot_path", StringType(), True),
    StructField("last_updated", TimestampType(), False)
])

# Track ALL games today (active and non-active) so finalization notebook can detect Final games
lifecycle_rows = [
    {
        "game_pk": game["gamePk"],
        "game_date": game["gameDate"][:10],
        "last_seen_status": game.get("status", {}).get("abstractGameState", "Unknown"),
        "final_snapshot_captured": False,
        "final_snapshot_time": None,
        "final_snapshot_path": None,
        "last_updated": datetime.utcnow()
    }
    for game in games
]

lifecycle_table_name = "game_lifecycle_state"

if lifecycle_rows:
    lifecycle_df = spark.createDataFrame(lifecycle_rows, schema=lifecycle_schema)
    try:
        lifecycle_table = DeltaTable.forName(spark, lifecycle_table_name)
        # Only update last_seen_status — never overwrite final_snapshot_captured set by finalization notebook
        lifecycle_table.alias("target").merge(
            lifecycle_df.alias("source"),
            "target.game_pk = source.game_pk"
        ).whenMatchedUpdate(
            set={
                "last_seen_status": "source.last_seen_status",
                "last_updated": "source.last_updated"
            }
        ).whenNotMatchedInsertAll(
        ).execute()
        print(f"   ✅ Lifecycle state updated for {len(lifecycle_rows)} games")
    except:
        lifecycle_df.write.format("delta").mode("overwrite").saveAsTable(lifecycle_table_name)
        print(f"   📝 Created lifecycle table with {len(lifecycle_rows)} games")

# ========================================
# Summary
# ========================================
print("\n" + "=" * 60)
print(f"✅ Incremental ingestion complete!")
print(f"   Total games today: {len(games)}")
print(f"   Active games: {len(active_games)}")
print(f"   Games with new plays: {len(files_saved)}")
print(f"   🌟 NEW PLAYS CAPTURED: {new_plays_total}")
print(f"   💾 Storage savings: ~{(1 - (new_plays_total / max(sum(f['total_plays'] for f in files_saved), 1))) * 100:.0f}% vs full fetch")
print("=" * 60)

# Return summary for pipeline
output = {
    "status": "success",
    "timestamp": timestamp,
    "total_games": len(games),
    "active_games": len(active_games),
    "games_with_new_plays": len(files_saved),
    "new_plays_captured": new_plays_total,
    "files_saved": len(files_saved),
    "files": files_saved,
    "incremental": True
}

mssparkutils.notebook.exit(json.dumps(output))
