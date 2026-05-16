# Fabric Troubleshooting Notebook
# Run this in MLB-Gold workspace to diagnose issues

import json
from pyspark.sql import SparkSession
from datetime import date

spark = SparkSession.builder.getOrCreate()

print("🔍 MLB Pipeline Diagnostics")
print("=" * 60)

# Test 1: Check Spark session
print("\n✅ Test 1: Spark session active")
print(f"   Spark version: {spark.version}")
print(f"   App name: {spark.sparkContext.appName}")

# Test 2: Check attached lakehouses
print("\n📂 Test 2: List available databases/lakehouses")
spark.sql("SHOW DATABASES").show(truncate=False)

# Test 3: Try to read from Silver (multiple strategies)
print("\n🔗 Test 3: Attempting to read Silver 'plays' table...")

SILVER_WORKSPACE = "MLB-Silver"
SILVER_LAKEHOUSE = "mlb_clean_playbyplay"
today = date.today().strftime("%Y-%m-%d")

# Strategy A: Attached lakehouse/shortcut
print("\n   Strategy A: Direct table reference (spark.table)")
try:
    df_a = spark.table("plays")
    count_a = df_a.count()
    print(f"   ✅ SUCCESS: Found {count_a} total plays")
    print(f"   Schema: {df_a.columns}")
    
    # Show sample
    print("\n   Sample data:")
    df_a.select("game_pk", "game_date", "home_team", "away_team", "event_description").show(5, truncate=False)
    
    # Filter for today
    today_plays = df_a.filter(df_a.game_date == today).count()
    print(f"\n   Plays for {today}: {today_plays}")
    
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    
    # Strategy B: OneLake path
    print("\n   Strategy B: OneLake cross-workspace path")
    try:
        silver_path = f"abfss://{SILVER_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE}.Lakehouse/Tables/plays"
        print(f"   Path: {silver_path}")
        
        df_b = spark.read.format("delta").load(silver_path)
        count_b = df_b.count()
        print(f"   ✅ SUCCESS: Found {count_b} total plays")
        
        # Filter for today
        today_plays = df_b.filter(df_b.game_date == today).count()
        print(f"   Plays for {today}: {today_plays}")
        
    except Exception as e2:
        print(f"   ❌ FAILED: {e2}")
        print("\n   💡 RECOMMENDATION:")
        print("      1. Verify Silver notebook completed successfully")
        print("      2. Check MLB-Silver lakehouse has 'plays' table")
        print("      3. Add mlb_clean_playbyplay lakehouse to this notebook")
        print("      4. OR create OneLake shortcut to Silver tables")

# Test 4: Check Gold lakehouse attachment
print("\n🏆 Test 4: Check Gold lakehouse (should be attached)")
try:
    # Try to show tables in current lakehouse
    tables = spark.sql("SHOW TABLES").collect()
    print(f"   Tables in attached lakehouse: {len(tables)}")
    for table in tables:
        print(f"      - {table.tableName}")
except Exception as e:
    print(f"   ⚠️  Warning: {e}")

print("\n" + "=" * 60)
print("✅ Diagnostics complete!")
print("=" * 60)
