# Fabric Notebook: Ingest MLB Play-by-Play Data
# Workspace: MLB-Bronze
# Lakehouse: mlb_raw_playbyplay
# Purpose: Fetch live game data from MLB Stats API and store raw JSON in Files folder

import requests
import json
from datetime import datetime, date
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
MLB_API_BASE = "https://statsapi.mlb.com/api"
TODAY = date.today().strftime("%Y-%m-%d")

print(f"🏈 MLB Play-by-Play Ingestion - {TODAY}")
print("=" * 60)

# Step 1: Get today's games
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

# Step 2: Filter active games
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

# Step 3: Fetch and store play-by-play for each active game
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
files_saved = []

for game in active_games:
    game_pk = game['gamePk']
    home_team = game['teams']['home']['team']['name']
    away_team = game['teams']['away']['team']['name']
    
    print(f"\n📥 Fetching: {away_team} @ {home_team} (Game {game_pk})")
    
    # Fetch live game feed
    game_url = f"{MLB_API_BASE}/v1.1/game/{game_pk}/feed/live"
    
    try:
        game_response = requests.get(game_url, timeout=30)
        game_response.raise_for_status()
        game_data = game_response.json()
        
        # Define storage path in OneLake Files folder
        game_date = game['gameDate'][:10]  # YYYY-MM-DD
        file_path = f"Files/raw/{game_date}/{game_pk}/{timestamp}.json"
        
        # Create directory structure if it doesn't exist
        dir_path = f"Files/raw/{game_date}/{game_pk}"
        mssparkutils.fs.mkdirs(dir_path)
        
        # Write to OneLake (using relative path when lakehouse is attached)
        json_content = json.dumps(game_data, indent=2)
        mssparkutils.fs.put(
            file_path,  # Use relative path, not full ABFS URL
            json_content,
            True  # Overwrite if exists
        )
        
        print(f"   ✅ Saved: {file_path}")
        files_saved.append({
            "game_pk": game_pk,
            "file_path": file_path,
            "home_team": home_team,
            "away_team": away_team,
            "timestamp": timestamp
        })
        
    except Exception as e:
        print(f"   ❌ Error fetching game {game_pk}: {e}")
        continue

# Summary
print("\n" + "=" * 60)
print(f"✅ Ingestion complete!")
print(f"   Total games today: {len(games)}")
print(f"   Active games: {len(active_games)}")
print(f"   Files saved: {len(files_saved)}")
print("=" * 60)

# Return summary for pipeline
output = {
    "status": "success",
    "timestamp": timestamp,
    "total_games": len(games),
    "active_games": len(active_games),
    "files_saved": len(files_saved),
    "files": files_saved
}

mssparkutils.notebook.exit(json.dumps(output))
# Workspace: MLB-Bronze
# Lakehouse: mlb_raw_playbyplay
# Purpose: Fetch live game data from MLB Stats API and store raw JSON in Files folder

import requests
import json
from datetime import datetime, date
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
MLB_API_BASE = "https://statsapi.mlb.com/api"
TODAY = date.today().strftime("%Y-%m-%d")

print(f"🏈 MLB Play-by-Play Ingestion - {TODAY}")
print("=" * 60)

# Step 1: Get today's games
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

# Step 2: Filter active games
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

# Step 3: Fetch and store play-by-play for each active game
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
files_saved = []

for game in active_games:
    game_pk = game['gamePk']
    home_team = game['teams']['home']['team']['name']
    away_team = game['teams']['away']['team']['name']
    
    print(f"\n📥 Fetching: {away_team} @ {home_team} (Game {game_pk})")
    
    # Fetch live game feed
    game_url = f"{MLB_API_BASE}/v1.1/game/{game_pk}/feed/live"
    
    try:
        game_response = requests.get(game_url, timeout=30)
        game_response.raise_for_status()
        game_data = game_response.json()
        
        # Define storage path in OneLake Files folder
        game_date = game['gameDate'][:10]  # YYYY-MM-DD
        file_path = f"Files/raw/{game_date}/{game_pk}/{timestamp}.json"
        
        # Write to OneLake (using mssparkutils for Fabric)
        json_content = json.dumps(game_data, indent=2)
        mssparkutils.fs.put(
            f"abfss://mlb_raw_playbyplay@onelake.dfs.fabric.microsoft.com/Files/raw/{game_date}/{game_pk}/{timestamp}.json",
            json_content,
            True  # Overwrite if exists
        )
        
        print(f"   ✅ Saved: {file_path}")
        files_saved.append({
            "game_pk": game_pk,
            "file_path": file_path,
            "home_team": home_team,
            "away_team": away_team,
            "timestamp": timestamp
        })
        
    except Exception as e:
        print(f"   ❌ Error fetching game {game_pk}: {e}")
        continue

# Summary
print("\n" + "=" * 60)
print(f"✅ Ingestion complete!")
print(f"   Total games today: {len(games)}")
print(f"   Active games: {len(active_games)}")
print(f"   Files saved: {len(files_saved)}")
print("=" * 60)

# Return summary for pipeline
output = {
    "status": "success",
    "timestamp": timestamp,
    "total_games": len(games),
    "active_games": len(active_games),
    "files_saved": len(files_saved),
    "files": files_saved
}

mssparkutils.notebook.exit(json.dumps(output))
