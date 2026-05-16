# MLB Pipeline → Microsoft Fabric Migration Guide

## Architecture Comparison

### Current (Azure)
```
MLB Stats API → ADF → Blob Storage → Azure SQL → (manual BI connection)
```

### Fabric (Proposed)
```
MLB Stats API → Fabric Pipeline → OneLake → Lakehouse Delta Tables → Power BI (auto-refresh)
```

---

## Migration Steps

### 1️⃣ Create Fabric Workspace & Lakehouse

**Via Fabric Portal:**
1. Go to https://app.fabric.microsoft.com
2. Create new Workspace: **"MLB Analytics"**
3. Create Lakehouse: **"mlb_plays"**
4. Note: OneLake automatically creates storage (no storage account setup needed!)

**Via MCP Tools (if available):**
```bash
# List workspaces
mcp_fabric_mcp_onelake_workspace_list

# Create lakehouse
mcp_fabric_mcp_onelake_item_create \
  --workspace "MLB Analytics" \
  --display-name "mlb_plays" \
  --item-type "Lakehouse"
```

---

### 2️⃣ Migrate Schema (SQL → Delta Lake)

**Create Delta Table in Lakehouse:**

Create a notebook in Fabric with this PySpark code:

```python
from pyspark.sql.types import *

# Define schema matching your current SQL table
schema = StructType([
    StructField("play_id", StringType(), False),
    StructField("game_pk", IntegerType(), False),
    StructField("inning", IntegerType(), True),
    StructField("half_inning", StringType(), True),
    StructField("at_bat_index", IntegerType(), True),
    StructField("play_count", IntegerType(), True),
    StructField("pitch_number", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_description", StringType(), True),
    StructField("batter_name", StringType(), True),
    StructField("pitcher_name", StringType(), True),
    StructField("home_score", IntegerType(), True),
    StructField("away_score", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("game_date", DateType(), True)
])

# Create empty Delta table
df = spark.createDataFrame([], schema)
df.write.format("delta").mode("overwrite").saveAsTable("mlb_plays")

print("✅ Delta table 'mlb_plays' created in Lakehouse")
```

---

### 3️⃣ Migrate Data Pipeline (ADF → Fabric Pipeline)

**Fabric Data Pipeline Components:**

**Pipeline: "MLB_PlayByPlay_Ingest"**

**Activity 1: Fetch Live Games**
- **Type:** Web Activity or Notebook
- **URL:** `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}`
- **Output:** List of game_pk values for active games

**Activity 2: For Each Active Game**
- **Type:** ForEach Activity
- **Items:** `@activity('Fetch Live Games').output.games`

**Activity 3: Fetch Play-by-Play Data**
- **Type:** Notebook Activity
- **Notebook:** `fetch_mlb_data` (see below)
- **Parameters:** `game_pk`

**Activity 4: Store Raw JSON**
- **Type:** Copy Activity
- **Destination:** `Files/raw/{game_date}/{game_pk}/{timestamp}.json`

**Activity 5: Load to Delta**
- **Type:** Notebook Activity
- **Notebook:** `load_to_delta` (see below)

---

### 4️⃣ Create Fabric Notebooks

**Notebook 1: `fetch_mlb_data.py`**

```python
# Parameters
game_pk = spark.conf.get("game_pk")
game_date = spark.conf.get("game_date")

import requests
from datetime import datetime

# Fetch live game feed
url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
response = requests.get(url)
data = response.json()

# Save raw JSON to OneLake Files folder
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
file_path = f"Files/raw/{game_date}/{game_pk}/{timestamp}.json"

# Write to OneLake
mssparkutils.fs.put(f"abfss://workspace@onelake.dfs.fabric.microsoft.com/mlb_plays/{file_path}", 
                    response.text, 
                    True)

# Return file path for next activity
output = {"file_path": file_path, "game_pk": game_pk}
mssparkutils.notebook.exit(output)
```

**Notebook 2: `load_to_delta.py`**

```python
from pyspark.sql.functions import *
from delta.tables import *

# Parameters
file_path = spark.conf.get("file_path")

# Read raw JSON
df = spark.read.json(f"Files/{file_path}")

# Transform and flatten plays
plays_df = df.select(
    col("liveData.plays.allPlays").alias("plays")
).select(
    explode("plays").alias("play")
).select(
    concat(
        col("play.about.atBatIndex").cast("string"),
        lit("_"),
        col("play.playEvents")[0]["index"].cast("string")
    ).alias("play_id"),
    col("gameData.game.pk").alias("game_pk"),
    col("play.about.inning").alias("inning"),
    col("play.about.halfInning").alias("half_inning"),
    col("play.about.atBatIndex").alias("at_bat_index"),
    col("play.playEvents")[0]["index"].alias("play_count"),
    col("play.playEvents")[0]["pitchNumber"].alias("pitch_number"),
    col("play.result.eventType").alias("event_type"),
    col("play.result.description").alias("event_description"),
    col("play.matchup.batter.fullName").alias("batter_name"),
    col("play.matchup.pitcher.fullName").alias("pitcher_name"),
    col("play.result.homeScore").alias("home_score"),
    col("play.result.awayScore").alias("away_score"),
    current_timestamp().alias("timestamp"),
    to_date(col("gameData.datetime.officialDate")).alias("game_date")
)

# MERGE into Delta table (upsert logic)
delta_table = DeltaTable.forName(spark, "mlb_plays")

delta_table.alias("target").merge(
    plays_df.alias("source"),
    "target.play_id = source.play_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print(f"✅ Loaded {plays_df.count()} plays into mlb_plays Delta table")
```

---

### 5️⃣ Create Power BI Report

**In Fabric Lakehouse:**
1. Go to your **mlb_plays** lakehouse
2. Click **"New semantic model"**
3. Select the `mlb_plays` Delta table
4. Click **"New report"**

**Sample Visuals:**
- **Card:** Total Plays, Active Games, Latest Score
- **Table:** Recent 10 plays with time, batter, event
- **Line Chart:** Score progression by inning
- **Matrix:** Plays by team and inning

**Enable Auto-Refresh:**
- Set refresh schedule (every 5-10 minutes)
- Or use DirectLake mode for near-real-time updates

---

### 6️⃣ Schedule Pipeline

**In Fabric Pipeline:**
1. Add **Schedule Trigger**
2. Set recurrence: **Every 5 minutes**
3. Start time: Game day start time
4. End time: Midnight

---

## Alternative: Real-Time Intelligence Option

For true streaming (sub-second latency):

### Create Eventstream

1. **Create Eventstream** in Fabric workspace
2. **Add Custom Endpoint** (HTTP endpoint)
3. **Connect to KQL Database**

### Python Streamer (replaces orchestrator)

```python
import requests
import time
from azure.eventhub import EventHubProducerClient, EventData

# Send to Fabric Eventstream
producer = EventHubProducerClient.from_connection_string(
    conn_str="<Eventstream connection string>",
    eventhub_name="mlb-plays"
)

while True:
    games = get_active_games()
    for game_pk in games:
        data = fetch_game_data(game_pk)
        event_data = EventData(json.dumps(data))
        producer.send_batch([event_data])
    
    time.sleep(30)  # Poll every 30 seconds for real-time
```

### KQL Database Queries

```kql
// Recent plays (auto-refreshing)
mlb_plays
| where timestamp > ago(10m)
| project timestamp, batter_name, event_description, home_score, away_score
| order by timestamp desc

// Live scoreboard
mlb_plays
| summarize arg_max(timestamp, home_score, away_score) by game_pk
| project game_pk, home_score, away_score
```

---

## Cost Comparison

### Current Azure Stack
- Azure Data Factory: ~$50-100/month (pipeline runs)
- Blob Storage: ~$5-20/month
- Azure SQL Database: ~$100-500/month (depends on tier)
- **Total: $155-620/month**

### Fabric
- Fabric Capacity F2 (smallest): **$262/month**
  - Includes: Lakehouses, Pipelines, Notebooks, Power BI, OneLake storage
  - All-inclusive, no per-service billing
- Can pause capacity when not in use (game days only)
- **Estimated: $50-150/month** (if paused off-season)

---

## Benefits of Fabric Migration

✅ **Unified Platform:** No juggling between ADF, Storage, SQL, Power BI  
✅ **OneLake:** No separate storage account setup  
✅ **Delta Lake:** Better performance, ACID transactions, time travel  
✅ **Built-in Power BI:** Auto-refresh, DirectLake mode  
✅ **Notebooks:** More flexible than ADF activities  
✅ **Collaboration:** Share workspace with team members  
✅ **Git Integration:** Version control for notebooks & pipelines  

---

## Migration Checklist

- [ ] Create Fabric workspace
- [ ] Create Lakehouse
- [ ] Migrate schema (SQL → Delta table)
- [ ] Create Fabric notebooks (fetch + load)
- [ ] Create Fabric Data Pipeline
- [ ] Test with single game
- [ ] Set up pipeline trigger
- [ ] Create Power BI report
- [ ] Decommission Azure resources

---

## Need Help?

I can help you with any of these steps! Let me know if you want to:
1. Create the Fabric workspace & lakehouse
2. Generate the notebook code
3. Build the Fabric pipeline
4. Create the Power BI report
5. Migrate your existing data
