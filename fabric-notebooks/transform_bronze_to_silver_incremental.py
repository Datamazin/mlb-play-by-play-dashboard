# Fabric Notebook: Transform Bronze to Silver (INCREMENTAL-AWARE)
# Workspace: MLB-Silver
# Lakehouse: mlb_clean_playbyplay
# Purpose: Process both full snapshots and incremental files from Bronze

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("🔄 Bronze → Silver Transformation (Incremental-Aware)")
print("=" * 60)

# Configuration
BRONZE_WORKSPACE_NAME = "MLB-Bronze"
BRONZE_LAKEHOUSE = "mlb_raw_playbyplay"

# Get input from pipeline parameter (optional - defaults to today)
try:
    game_date = spark.conf.get("game_date")
except:
    from datetime import date
    game_date = date.today().strftime("%Y-%m-%d")

print(f"📅 Processing date: {game_date}")

# ========================================
# Step 1: Read JSON files from Bronze
# ========================================
bronze_path = f"abfss://{BRONZE_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE}.Lakehouse/Files/raw"
json_path = f"{bronze_path}/{game_date}/*/*.json"
print(f"\n📂 Reading from Bronze: {json_path}")

try:
    raw_df = spark.read.option("multiline", "true").json(json_path)
    file_count = raw_df.count()
    print(f"✅ Loaded {file_count} JSON files")
    
    # Check for incremental files
    incremental_count = raw_df.filter(col("metadata.incremental") == True).count()
    full_count = file_count - incremental_count
    
    print(f"   📊 Full snapshots: {full_count}")
    print(f"   ⚡ Incremental files: {incremental_count}")
    
except Exception as e:
    print(f"❌ Error reading Bronze data: {e}")
    print("ℹ️  This might mean no games were played today")
    mssparkutils.notebook.exit('{"status": "no_data"}')

# ========================================
# Step 2: Extract and Flatten Play Data
# ========================================
print("\n🔧 Transforming data...")

# Handle both incremental and full snapshot files
# The structure is the same, just different play counts
plays_df = raw_df.select(
    col("gameData.game.pk").alias("game_pk"),
    col("gameData.datetime.officialDate").alias("game_date"),
    col("gameData.teams.home.name").alias("home_team"),
    col("gameData.teams.away.name").alias("away_team"),
    col("gameData.venue.name").alias("venue"),
    
    # Metadata to track incremental vs full
    col("metadata.incremental").alias("is_incremental"),
    col("metadata.play_range").alias("play_range"),
    col("metadata.capture_time").alias("capture_time"),
    
    explode("liveData.plays.allPlays").alias("play")
).select(
    col("game_pk"),
    col("game_date"),
    col("home_team"),
    col("away_team"),
    col("venue"),
    col("is_incremental"),
    col("play_range"),
    col("capture_time"),
    
    # Play identification - unique across all batches
    concat(
        col("game_pk").cast("string"),
        lit("_"),
        col("play.about.atBatIndex").cast("string"),
        lit("_"),
        coalesce(col("play.playEvents")[0]["index"], lit(0)).cast("string")
    ).alias("play_id"),
    
    # Play details
    col("play.about.inning").alias("inning"),
    col("play.about.halfInning").alias("half_inning"),
    col("play.about.atBatIndex").alias("at_bat_index"),
    coalesce(col("play.playEvents")[0]["index"], lit(0)).alias("play_count"),
    coalesce(col("play.playEvents")[0]["pitchNumber"], lit(0)).alias("pitch_number"),
    
    # Event information
    col("play.result.event").alias("event_type"),
    col("play.result.eventType").alias("event_category"),
    col("play.result.description").alias("event_description"),
    
    # Players
    col("play.matchup.batter.fullName").alias("batter_name"),
    col("play.matchup.batter.id").alias("batter_id"),
    col("play.matchup.pitcher.fullName").alias("pitcher_name"),
    col("play.matchup.pitcher.id").alias("pitcher_id"),
    
    # Scores
    coalesce(col("play.result.homeScore"), lit(0)).alias("home_score"),
    coalesce(col("play.result.awayScore"), lit(0)).alias("away_score"),
    
    # Runners on base
    col("play.matchup.postOnFirst.fullName").alias("runner_on_first"),
    col("play.matchup.postOnSecond.fullName").alias("runner_on_second"),
    col("play.matchup.postOnThird.fullName").alias("runner_on_third"),
    
    # Timestamp
    current_timestamp().alias("processed_timestamp")
)

# ========================================
# Step 3: Data Quality & Deduplication
# ========================================
print("🧹 Cleaning data...")

# Remove nulls and invalid records
clean_df = plays_df.filter(
    col("play_id").isNotNull() &
    col("game_pk").isNotNull() &
    col("inning").isNotNull()
).dropDuplicates(["play_id"])  # Critical: dedupe by play_id across all files

initial_count = plays_df.count()
clean_count = clean_df.count()
duplicates_removed = initial_count - clean_count

print(f"✅ Cleaned {clean_count} plays")
if duplicates_removed > 0:
    print(f"   🗑️  Removed {duplicates_removed} duplicates")

# Show incremental stats
incremental_plays = clean_df.filter(col("is_incremental") == True).count()
full_plays = clean_count - incremental_plays

print(f"   📊 From full snapshots: {full_plays}")
print(f"   ⚡ From incremental: {incremental_plays}")

# ========================================
# Step 4: MERGE to Silver Delta Table
# ========================================
print("\n💾 Writing to Silver lakehouse...")

try:
    silver_table = DeltaTable.forName(spark, "plays")
    print("   📋 Table exists - performing MERGE (upsert)")
    
    # MERGE operation - handles both incremental and full data
    # play_id is unique, so duplicates across batches are automatically handled
    silver_table.alias("target").merge(
        clean_df.alias("source"),
        "target.play_id = source.play_id"
    ).whenMatchedUpdate(
        # Update if source has more recent timestamp
        condition="source.processed_timestamp > target.processed_timestamp",
        set={
            "home_score": "source.home_score",
            "away_score": "source.away_score",
            "processed_timestamp": "source.processed_timestamp"
        }
    ).whenNotMatchedInsertAll(
    ).execute()
    
    print(f"   ✅ MERGE complete")
    
except Exception as e:
    print(f"   📝 Table doesn't exist - creating new table")
    # Create table if it doesn't exist
    clean_df.write.format("delta").mode("overwrite").saveAsTable("plays")
    print(f"   ✅ Created new table")

# Get final row count
final_count = spark.table("plays").count()
print(f"✅ Silver table now contains: {final_count} total plays")

# ========================================
# Summary
# ========================================
print("\n" + "=" * 60)
print(f"✅ Bronze → Silver transformation complete!")
print(f"   Game date: {game_date}")
print(f"   Files processed: {file_count}")
print(f"   Incremental files: {incremental_count}")
print(f"   Plays processed this run: {clean_count}")
print(f"   Total plays in Silver: {final_count}")
print("=" * 60)

# Return summary
output = {
    "status": "success",
    "game_date": game_date,
    "files_processed": file_count,
    "incremental_files": incremental_count,
    "plays_processed": clean_count,
    "total_plays_in_silver": final_count,
    "duplicates_removed": duplicates_removed
}

mssparkutils.notebook.exit(json.dumps(output))
