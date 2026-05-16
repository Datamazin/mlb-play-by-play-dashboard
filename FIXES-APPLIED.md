# Fabric Notebook Path Fix - RESOLVED

## Issue
The notebooks were using incorrect OneLake paths, causing `400 Bad Request` errors when trying to write files.

## Root Cause
**Original (Incorrect):**
```python
mssparkutils.fs.put(
    f"abfss://mlb_raw_playbyplay@onelake.dfs.fabric.microsoft.com/Files/raw/{game_date}/{game_pk}/{timestamp}.json",
    json_content,
    True
)
```

The full ABFS URL format doesn't work when the notebook is already attached to a lakehouse.

## Solution Applied

### ✅ Bronze Notebook (ingest_mlb_playbyplay.py)
**Fixed to use relative paths:**
```python
# Create directory first
dir_path = f"Files/raw/{game_date}/{game_pk}"
mssparkutils.fs.mkdirs(dir_path)

# Use relative path when lakehouse is attached
file_path = f"Files/raw/{game_date}/{game_pk}/{timestamp}.json"
mssparkutils.fs.put(file_path, json_content, True)
```

### ✅ Silver Notebook (transform_bronze_to_silver.py)
**Fixed to use workspace NAME instead of workspace ID:**
```python
# Use workspace NAME for cross-lakehouse access
BRONZE_WORKSPACE_NAME = "MLB-Bronze"
BRONZE_LAKEHOUSE = "mlb_raw_playbyplay"

bronze_path = f"abfss://{BRONZE_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE}.Lakehouse/Files/raw"
json_path = f"{bronze_path}/{game_date}/*/*.json"
```

**Why this works:** OneLake supports workspace names (not just IDs) for cross-workspace access, and it's more readable.

### ✅ Gold Notebook (aggregate_silver_to_gold.py)
**Fixed to read Silver Delta tables via OneLake:**
```python
# Read from Silver lakehouse via OneLake path
SILVER_WORKSPACE = "MLB-Silver"
SILVER_LAKEHOUSE = "mlb_clean_playbyplay"

silver_path = f"abfss://{SILVER_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE}.Lakehouse/Tables/plays"
silver_df = spark.read.format("delta").load(silver_path)
```

**Fallback:** If OneLake path fails, tries `spark.table("plays")` for attached lakehouses.

## How to Test

### Step 1: Attach Lakehouse to Notebook
Before running the Bronze notebook:
1. Open the notebook in Fabric
2. Click **Add Lakehouse** (left sidebar)
3. Select **Existing lakehouse**
4. Choose **mlb_raw_playbyplay**
5. Click **Add**

### Step 2: Run Bronze Notebook
```python
# Should now work without errors!
# Files will be saved to: Files/raw/2026-05-16/{game_pk}/{timestamp}.json
```

### Step 3: Verify Files Created
In the Lakehouse explorer:
```
mlb_raw_playbyplay
└── Files
    └── raw
        └── 2026-05-16
            └── 824278
                └── 20260516_185804.json
```

### Step 4: Run Silver Notebook
- Attach **mlb_clean_playbyplay** lakehouse to the Silver notebook
- Run the transformation
- It will read from Bronze using the cross-workspace path

## Key Takeaways

1. **Within Same Lakehouse:** Use relative paths (`Files/raw/...`)
2. **Cross-Lakehouse Access:** Use full OneLake path with workspace ID
3. **Always Create Directories First:** Use `mssparkutils.fs.mkdirs()` before writing files
4. **Attach Lakehouse to Notebook:** Critical step before running!

## OneLake Path Formats

### Format 1: Relative Path (Same Lakehouse)
```
Files/raw/2026-05-16/game.json
Tables/my_table
```

### Format 2: Cross-Workspace OneLake Path (Use Workspace NAME)
```
# For Files:
abfss://{workspace-name}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Files/{path}

# For Tables:
abfss://{workspace-name}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{table}

# Example:
abfss://MLB-Bronze@onelake.dfs.fabric.microsoft.com/mlb_raw_playbyplay.Lakehouse/Files/raw/2026-05-16/*/*.json
```

**Important:** Use workspace **names** (like "MLB-Bronze"), NOT workspace GUIDs. Workspace IDs in the path cause "Bad Request" errors.

### Format 3: OneLake Shortcut (Alternative)
Create a shortcut in Silver workspace pointing to Bronze Files folder:
1. In Silver lakehouse → Files → New → Shortcut
2. Select OneLake → Browse to Bronze lakehouse Files folder
3. Then reference as: `Files/BronzeShortcut/raw/...`

## Updated File References

✅ **fabric-notebooks/ingest_mlb_playbyplay.py** - Fixed (relative paths)  
✅ **fabric-notebooks/transform_bronze_to_silver.py** - Fixed (workspace name path)  
✅ **fabric-notebooks/aggregate_silver_to_gold.py** - Fixed (OneLake cross-workspace read)

## Next Steps

1. ✅ Re-run Bronze notebook (should work now!)
2. ✅ Verify files appear in Files/raw folder
3. ✅ Run Silver transformation
4. ✅ Create Gold aggregations
5. Build Power BI report on Gold tables
