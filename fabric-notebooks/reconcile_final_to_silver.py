# Fabric Notebook: Reconcile Final Snapshots to Silver
# Workspace: MLB-Silver
# Lakehouse: mlb_clean_playbyplay
# Purpose: Process canonical final game snapshots into silver_plays_final and write closure audit rows

import json
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

BRONZE_WORKSPACE = "MLB-Bronze"
BRONZE_LAKEHOUSE = "mlb_raw_playbyplay"

try:
    game_date = spark.conf.get("game_date")
except:
    game_date = date.today().strftime("%Y-%m-%d")

print("🔄 Final Snapshot → Silver Reconciliation")
print("=" * 60)
print(f"📅 Processing date: {game_date}")

# ========================================
# Step 1: Read final snapshot files from Bronze
# ========================================
bronze_path = f"abfss://{BRONZE_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE}.Lakehouse/Files/raw"
final_path = f"{bronze_path}/{game_date}/*/final_*.json"

print(f"\n📂 Reading final snapshots: {final_path}")

try:
    raw_df = spark.read.option("multiline", "true").json(final_path)
    file_count = raw_df.count()
    print(f"✅ Loaded {file_count} final snapshot file(s)")
except Exception as e:
    print(f"⚠️  No final snapshot files found for {game_date}: {e}")
    mssparkutils.notebook.exit(json.dumps({"status": "no_final_snapshots", "game_date": game_date}))

if file_count == 0:
    print("ℹ️  No final snapshot files for this date - exiting")
    mssparkutils.notebook.exit(json.dumps({"status": "no_final_snapshots", "game_date": game_date}))

# ========================================
# Step 2: Flatten plays (mirrors transform_bronze_to_silver_incremental logic)
# ========================================
print("\n🔧 Flattening plays from final snapshots...")

plays_df = raw_df.select(
    col("gameData.game.pk").alias("game_pk"),
    col("gameData.datetime.officialDate").alias("game_date"),
    col("gameData.teams.home.name").alias("home_team"),
    col("gameData.teams.away.name").alias("away_team"),
    col("gameData.venue.name").alias("venue"),
    col("_snapshot_metadata.capture_time").alias("capture_time"),
    col("_snapshot_metadata.total_plays").alias("snapshot_total_plays"),
    explode("liveData.plays.allPlays").alias("play")
).select(
    col("game_pk"),
    col("game_date"),
    col("home_team"),
    col("away_team"),
    col("venue"),
    col("capture_time"),
    col("snapshot_total_plays"),

    # play_id uses same key as incremental Silver — enables cross-table dedup if needed
    concat(
        col("game_pk").cast("string"), lit("_"),
        col("play.about.atBatIndex").cast("string"), lit("_"),
        coalesce(col("play.playEvents")[0]["index"], lit(0)).cast("string")
    ).alias("play_id"),

    col("play.about.inning").alias("inning"),
    col("play.about.halfInning").alias("half_inning"),
    col("play.about.atBatIndex").alias("at_bat_index"),
    coalesce(col("play.playEvents")[0]["index"], lit(0)).alias("play_count"),
    coalesce(col("play.playEvents")[0]["pitchNumber"], lit(0)).alias("pitch_number"),
    col("play.result.event").alias("event_type"),
    col("play.result.eventType").alias("event_category"),
    col("play.result.description").alias("event_description"),
    col("play.matchup.batter.fullName").alias("batter_name"),
    col("play.matchup.batter.id").alias("batter_id"),
    col("play.matchup.pitcher.fullName").alias("pitcher_name"),
    col("play.matchup.pitcher.id").alias("pitcher_id"),
    coalesce(col("play.result.homeScore"), lit(0)).alias("home_score"),
    coalesce(col("play.result.awayScore"), lit(0)).alias("away_score"),
    col("play.matchup.postOnFirst.fullName").alias("runner_on_first"),
    col("play.matchup.postOnSecond.fullName").alias("runner_on_second"),
    col("play.matchup.postOnThird.fullName").alias("runner_on_third"),
    lit("final").alias("source_snapshot"),
    current_timestamp().alias("processed_timestamp")
)

clean_df = plays_df.filter(
    col("play_id").isNotNull() &
    col("game_pk").isNotNull() &
    col("inning").isNotNull()
).dropDuplicates(["play_id"])

silver_play_count = clean_df.count()
print(f"✅ {silver_play_count} plays extracted and cleaned")

# ========================================
# Step 3: MERGE into silver_plays_final
# ========================================
print("\n💾 Merging into silver_plays_final...")

try:
    final_table = DeltaTable.forName(spark, "silver_plays_final")
    final_table.alias("target").merge(
        clean_df.alias("source"),
        "target.play_id = source.play_id"
    ).whenMatchedUpdate(
        condition="source.processed_timestamp > target.processed_timestamp",
        set={
            "home_score": "source.home_score",
            "away_score": "source.away_score",
            "processed_timestamp": "source.processed_timestamp"
        }
    ).whenNotMatchedInsertAll(
    ).execute()
    print(f"   ✅ MERGE into silver_plays_final complete")
except:
    clean_df.write.format("delta").mode("overwrite").saveAsTable("silver_plays_final")
    print(f"   📝 Created silver_plays_final table")

total_final = spark.table("silver_plays_final").count()
print(f"✅ silver_plays_final now contains {total_final} plays")

# ========================================
# Step 4: Write game closure audit rows
# ========================================
print("\n📋 Writing game closure audit...")

audit_schema = StructType([
    StructField("game_pk", IntegerType(), False),
    StructField("game_date", StringType(), False),
    StructField("expected_play_count", IntegerType(), True),
    StructField("silver_play_count", IntegerType(), False),
    StructField("matched", BooleanType(), False),
    StructField("audit_time", TimestampType(), False)
])

# Expected counts come from the raw final snapshot metadata
expected_counts = {
    row["game_pk"]: row["snapshot_total_plays"]
    for row in raw_df.select(
        col("gameData.game.pk").alias("game_pk"),
        col("_snapshot_metadata.total_plays").alias("snapshot_total_plays")
    ).collect()
}

# Actual counts from silver_plays_final for this date
silver_counts = {
    row["game_pk"]: row["count"]
    for row in spark.table("silver_plays_final")
    .filter(col("game_date") == game_date)
    .groupBy("game_pk").count().collect()
}

audit_rows = [
    {
        "game_pk": game_pk,
        "game_date": game_date,
        "expected_play_count": expected,
        "silver_play_count": silver_counts.get(game_pk, 0),
        "matched": (expected == silver_counts.get(game_pk, 0)),
        "audit_time": datetime.utcnow()
    }
    for game_pk, expected in expected_counts.items()
]

if audit_rows:
    audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)
    try:
        DeltaTable.forName(spark, "silver_game_closure_audit").alias("t").merge(
            audit_df.alias("s"),
            "t.game_pk = s.game_pk AND t.game_date = s.game_date"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except:
        audit_df.write.format("delta").mode("overwrite").saveAsTable("silver_game_closure_audit")

    for row in audit_rows:
        icon = "✅" if row["matched"] else "⚠️  MISMATCH"
        print(f"   {icon} Game {row['game_pk']}: expected {row['expected_play_count']}, silver {row['silver_play_count']}")

# ========================================
# Summary
# ========================================
print("\n" + "=" * 60)
print(f"✅ Final reconciliation complete!")
print(f"   Game date:                 {game_date}")
print(f"   Snapshot files processed:  {file_count}")
print(f"   Plays merged (this run):   {silver_play_count}")
print(f"   Total in silver_plays_final: {total_final}")
print(f"   Audit rows written:        {len(audit_rows)}")
print("=" * 60)

output = {
    "status": "success",
    "game_date": game_date,
    "files_processed": file_count,
    "plays_merged": silver_play_count,
    "total_silver_final": total_final,
    "audit": audit_rows
}

mssparkutils.notebook.exit(json.dumps(output))
