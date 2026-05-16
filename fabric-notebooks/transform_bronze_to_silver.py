# Fabric Notebook: Transform Bronze to Silver
# Workspace: MLB-Silver
# Lakehouse: mlb_clean_playbyplay
# Purpose: Read raw JSON from Bronze, transform, and write to Silver Delta tables

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("🔄 Bronze → Silver Transformation")
print("=" * 60)

# Configuration - Use workspace name for OneLake cross-workspace access
BRONZE_WORKSPACE_NAME = "MLB-Bronze"
BRONZE_LAKEHOUSE = "mlb_raw_playbyplay"

# Get input from pipeline parameter (optional - defaults to today)
try:
    game_date = spark.conf.get("game_date")
except:
    from datetime import date
    game_date = date.today().strftime("%Y-%m-%d")

print(f"📅 Processing date: {game_date}")

# Step 1: Read raw JSON files from Bronze using OneLake path
# OneLake path format: abfss://<workspace-name>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Files/<path>
bronze_path = f"abfss://{BRONZE_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE}.Lakehouse/Files/raw"
json_path = f"{bronze_path}/{game_date}/*/*.json"
print(f"\n📂 Reading from Bronze: {json_path}")

try:
    raw_df = spark.read.option("multiline", "true").json(json_path)
    print(f"✅ Loaded {raw_df.count()} raw files")
except Exception as e:
    print(f"❌ Error reading Bronze data: {e}")
    print("ℹ️  This might mean no games were played today")
    mssparkutils.notebook.exit('{"status": "no_data"}')

# Step 2: Extract and flatten play-by-play data
print("\n🔧 Transforming data...")

# Flatten nested JSON structure
plays_df = raw_df.select(
    col("gameData.game.pk").alias("game_pk"),
    col("gameData.datetime.officialDate").alias("game_date"),
    col("gameData.teams.home.name").alias("home_team"),
    col("gameData.teams.away.name").alias("away_team"),
    col("gameData.venue.name").alias("venue"),
    explode("liveData.plays.allPlays").alias("play")
).select(
    col("game_pk"),
    col("game_date"),
    col("home_team"),
    col("away_team"),
    col("venue"),
    
    # Play identification
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

# Step 3: Data quality checks and cleaning
print("🧹 Cleaning data...")

# Remove nulls and invalid records
clean_df = plays_df.filter(
    col("play_id").isNotNull() &
    col("game_pk").isNotNull() &
    col("inning").isNotNull()
).dropDuplicates(["play_id"])

print(f"✅ Cleaned {clean_df.count()} plays")

# Step 4: Write to Silver Delta table (MERGE upsert)
print("\n💾 Writing to Silver lakehouse...")

# Ensure Delta table exists
try:
    silver_table = DeltaTable.forName(spark, "mlb_clean_playbyplay.plays")
    print("   📋 Table exists - performing MERGE")
    
    # MERGE (upsert) operation
    silver_table.alias("target").merge(
        clean_df.alias("source"),
        "target.play_id = source.play_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()
    
except Exception as e:
    print(f"   📝 Table doesn't exist - creating new table")
    # Create table if it doesn't exist
    clean_df.write.format("delta").mode("overwrite").saveAsTable("plays")

record_count = clean_df.count()
print(f"✅ Silver table updated: {record_count} plays")

# Summary
print("\n" + "=" * 60)
print(f"✅ Bronze → Silver transformation complete!")
print(f"   Game date: {game_date}")
print(f"   Plays processed: {record_count}")
print("=" * 60)

# Return summary
output = {
    "status": "success",
    "game_date": game_date,
    "plays_processed": record_count
}

mssparkutils.notebook.exit(json.dumps(output))
