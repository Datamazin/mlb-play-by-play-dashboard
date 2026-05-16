# Fabric Notebook: Aggregate Silver to Gold
# Workspace: MLB-Gold
# Lakehouse: mlb_analytics
# Purpose: Create analytics-ready aggregations and business logic

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

print("📊 Silver → Gold Aggregation")
print("=" * 60)

# Configuration - point to Silver lakehouse
SILVER_WORKSPACE = "MLB-Silver"
SILVER_LAKEHOUSE = "mlb_clean_playbyplay"

# Get input from pipeline parameter
try:
    game_date = spark.conf.get("game_date")
except:
    from datetime import date
    game_date = date.today().strftime("%Y-%m-%d")

print(f"📅 Processing date: {game_date}")

# Step 1: Read from Silver
print(f"\n📂 Reading from Silver lakehouse...")

try:
    # Strategy 1: Try reading from attached Silver lakehouse (if added as shortcut/reference)
    try:
        silver_df = spark.table("plays")
        print(f"✅ Read from Silver via attached lakehouse")
    except:
        # Strategy 2: Use OneLake cross-workspace path
        print(f"   Trying OneLake cross-workspace path...")
        silver_path = f"abfss://{SILVER_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE}.Lakehouse/Tables/plays"
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"✅ Read from Silver via OneLake path")
    
    # Filter for today's games
    plays_df = silver_df.filter(col("game_date") == game_date)
    
    play_count = plays_df.count()
    print(f"✅ Loaded {play_count} plays for {game_date}")
    
    if play_count == 0:
        print("⚠️  No plays found for this date")
        mssparkutils.notebook.exit('{"status": "no_data", "reason": "no_plays_for_date"}')
    
except Exception as e:
    print(f"❌ Error reading Silver data: {e}")
    print("\n💡 Troubleshooting tips:")
    print("   1. Verify Silver notebook ran successfully")
    print("   2. Check 'plays' table exists in MLB-Silver lakehouse")
    print("   3. Attach mlb_clean_playbyplay lakehouse to this notebook")
    print("   4. Or create OneLake shortcut to Silver tables")
    mssparkutils.notebook.exit('{"status": "error", "message": "Cannot read Silver data"}')

# Step 2: Create aggregations for analytics

# === Aggregation 1: Live Scoreboard ===
print("\n📊 Creating live scoreboard...")

scoreboard_df = plays_df.groupBy("game_pk", "home_team", "away_team", "venue").agg(
    max("home_score").alias("home_score"),
    max("away_score").alias("away_score"),
    max("inning").alias("current_inning"),
    max(when(col("half_inning") == "top", 1).otherwise(2)).alias("half_inning_num"),
    count("*").alias("total_plays"),
    max("processed_timestamp").alias("last_updated")
).withColumn(
    "game_status",
    when(col("current_inning") >= 9, 
         when(col("home_score") != col("away_score"), "Final")
         .otherwise("In Progress"))
    .otherwise("In Progress")
).withColumn(
    "winning_team",
    when(col("home_score") > col("away_score"), col("home_team"))
    .when(col("away_score") > col("home_score"), col("away_team"))
    .otherwise("Tie")
)

# === Aggregation 2: Recent Plays View ===
print("📋 Creating recent plays view...")

window_spec = Window.partitionBy("game_pk").orderBy(desc("at_bat_index"), desc("play_count"))

recent_plays_df = plays_df.withColumn(
    "play_rank",
    row_number().over(window_spec)
).filter(
    col("play_rank") <= 10
).select(
    "game_pk",
    "home_team",
    "away_team",
    "inning",
    "half_inning",
    "event_description",
    "batter_name",
    "pitcher_name",
    "home_score",
    "away_score",
    "processed_timestamp"
).orderBy("game_pk", "play_rank")

# === Aggregation 3: Scoring Plays ===
print("⚾ Creating scoring plays summary...")

# Detect scoring plays (score change from previous play)
window_score = Window.partitionBy("game_pk").orderBy("at_bat_index", "play_count")

scoring_plays_df = plays_df.withColumn(
    "prev_home_score",
    lag("home_score", 1).over(window_score)
).withColumn(
    "prev_away_score",
    lag("away_score", 1).over(window_score)
).withColumn(
    "runs_scored",
    (col("home_score") - coalesce(col("prev_home_score"), lit(0))) +
    (col("away_score") - coalesce(col("prev_away_score"), lit(0)))
).filter(
    col("runs_scored") > 0
).select(
    "game_pk",
    "home_team",
    "away_team",
    "inning",
    "half_inning",
    "event_description",
    "batter_name",
    "runs_scored",
    "home_score",
    "away_score",
    "processed_timestamp"
)

# === Aggregation 4: Player Statistics ===
print("👤 Creating player statistics...")

batter_stats_df = plays_df.groupBy("game_pk", "home_team", "away_team", "batter_name", "batter_id").agg(
    count("*").alias("at_bats"),
    sum(when(col("event_type").isin(["Single", "Double", "Triple", "Home Run"]), 1).otherwise(0)).alias("hits"),
    sum(when(col("event_type") == "Home Run", 1).otherwise(0)).alias("home_runs"),
    sum(when(col("event_type").isin(["Strikeout", "Strikeout - DP"]), 1).otherwise(0)).alias("strikeouts")
).withColumn(
    "batting_average",
    round(col("hits") / col("at_bats"), 3)
)

# Step 3: Write to Gold Delta tables
print("\n💾 Writing to Gold lakehouse...")

# Write each aggregation to separate tables
try:
    # Scoreboard
    scoreboard_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("live_scoreboard")
    print(f"   ✅ Scoreboard: {scoreboard_df.count()} games")
    
    # Recent plays
    recent_plays_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("recent_plays")
    print(f"   ✅ Recent plays: {recent_plays_df.count()} plays")
    
    # Scoring plays
    scoring_plays_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("scoring_plays")
    print(f"   ✅ Scoring plays: {scoring_plays_df.count()} plays")
    
    # Batter stats
    batter_stats_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("batter_statistics")
    print(f"   ✅ Batter stats: {batter_stats_df.count()} batters")
    
except Exception as e:
    print(f"❌ Error writing to Gold: {e}")
    raise

# Summary
print("\n" + "=" * 60)
print(f"✅ Silver → Gold aggregation complete!")
print(f"   Game date: {game_date}")
print(f"   Games: {scoreboard_df.count()}")
print(f"   Total plays processed: {plays_df.count()}")
print("=" * 60)

# Return summary
output = {
    "status": "success",
    "game_date": game_date,
    "games": scoreboard_df.count(),
    "total_plays": plays_df.count(),
    "scoring_plays": scoring_plays_df.count()
}

mssparkutils.notebook.exit(json.dumps(output))
