# MLB Play-by-Play Data Pipeline

## Architecture

```
MLB Stats API (free, no auth)
        │  every 5 min
        ▼
┌─────────────────────────────┐
│   Azure Data Factory        │  ← ARM template deploys this
│   Pipeline + 5-min trigger  │
└────────────┬────────────────┘
             │ Copy Activity 1
             ▼
┌─────────────────────────────┐
│   Azure Blob Storage        │  mlb-raw/YYYY/MM/DD/<game_pk>/<ts>.json
│   (raw JSON landing zone)   │  — immutable audit trail
└────────────┬────────────────┘
             │ Copy Activity 2 (via stored procedure)
             ▼
┌─────────────────────────────┐
│   Azure SQL Database        │  dbo.mlb_plays  (MERGE upsert)
│   dbo.mlb_plays             │  dbo.vw_live_scores
│   dbo.vw_recent_plays       │  dbo.vw_recent_plays
└─────────────────────────────┘
```

## Files

|File                      |Purpose                                                  |
|--------------------------|---------------------------------------------------------|
|`pipeline_orchestrator.py`|Standalone Python runner (dev / container)               |
|`adf_arm_template.json`   |ADF factory, linked services, datasets, pipeline, trigger|
|`schema.sql`              |Azure SQL table, TVP type, upsert SP, monitoring views   |
|`requirements.txt`        |Python dependencies                                      |

-----

## Quick Start

### 1. Deploy Azure SQL Schema

```bash
sqlcmd -S <server>.database.windows.net \
       -d mlb_analytics \
       -U <user> -P <password> \
       -i schema.sql
```

### 2. Deploy ADF via ARM Template

```bash
az deployment group create \
  --resource-group mlb-rg \
  --template-file adf_arm_template.json \
  --parameters \
      storageAccountName=<your_storage> \
      sqlServerName=<your_sql_server> \
      sqlAdminUser=<user> \
      sqlAdminPassword=<password>
```

### 3. Run Locally (dev/testing)

```bash
pip install -r requirements.txt

export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
export AZURE_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};..."

python pipeline_orchestrator.py
```

-----

## Environment Variables

|Variable                         |Description                           |
|---------------------------------|--------------------------------------|
|`AZURE_STORAGE_CONNECTION_STRING`|Blob Storage connection string        |
|`AZURE_SQL_CONNECTION_STRING`    |pyodbc connection string for Azure SQL|
|`POLL_INTERVAL_SECONDS`          |Seconds between polls (default: 300)  |

-----

## Data Source

**MLB Stats API** — free, no API key required  
Base URL: `https://statsapi.mlb.com/api/v1`

Key endpoints used:

- `GET /schedule?sportId=1&date=YYYY-MM-DD` — today’s games
- `GET /v1.1/game/{game_pk}/feed/live` — full play-by-play live feed

-----

## Key Design Decisions

- **MERGE upsert** — re-running the pipeline is safe; duplicate plays are updated, not duplicated.
- **Blob as landing zone** — raw JSON is always stored first, enabling reprocessing if SQL schema changes.
- **Active games only** — the orchestrator skips games with status outside `{In Progress, Warmup, Pre-Game}` to reduce unnecessary API calls.
- **Idempotent** — pipeline can be run multiple times without side effects.