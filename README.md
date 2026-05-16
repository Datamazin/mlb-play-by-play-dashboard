# ⚾ MLB Play-by-Play Data Pipeline

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-Ready-blue?logo=microsoft)](https://fabric.microsoft.com)
[![MLB Stats API](https://img.shields.io/badge/MLB_Stats_API-Free-green)](https://statsapi.mlb.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Format-orange)](https://delta.io)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Real-time MLB play-by-play data pipeline built on **Microsoft Fabric** with medallion architecture (Bronze → Silver → Gold). Features **incremental ingestion** for 88% storage savings and 40% faster processing.

---

## 🎯 Overview

Automated data pipeline that ingests live MLB game data every 5 minutes during game hours (12 PM - 2 AM Eastern), transforms it through Bronze/Silver/Gold layers, and serves analytics-ready tables for Power BI dashboards.

**Key Features:**
- ⚡ **Incremental ingestion** with checkpoint tracking (88% storage reduction)
- 🏗️ **Medallion architecture** (Bronze/Silver/Gold) with Delta Lake
- 🔄 **Real-time updates** every 5 minutes during games
- 💰 **Cost optimized**: $405/year savings vs full snapshot approach
- 📊 **Power BI ready** analytics tables

---

## 🏗️ Architecture

### Modern Implementation (Microsoft Fabric)

```
MLB Stats API (free, no auth)
        │  every 5 min
        ▼
┌─────────────────────────────┐
│   🥉 BRONZE (MLB-Bronze)    │  Incremental ingestion with checkpoints
│   ├─ game_checkpoints       │  ← Tracks last processed play per game
│   └─ Files/raw/{date}/{id}/ │  ← Only NEW plays stored (88% savings)
└────────────┬────────────────┘
             │ Transform
             ▼
┌─────────────────────────────┐
│   🥈 SILVER (MLB-Silver)    │  Cleaned Delta tables
│   └─ plays (Delta)          │  ← Deduplicated via MERGE on play_id
└────────────┬────────────────┘
             │ Aggregate
             ▼
┌─────────────────────────────┐
│   🥇 GOLD (MLB-Gold)        │  Analytics tables
│   ├─ live_scoreboard        │  ← Current game scores
│   ├─ recent_plays           │  ← Last 50 plays
│   ├─ scoring_plays          │  ← All scoring events
│   └─ batter_statistics      │  ← Player performance
└─────────────────────────────┘
```

---

## 📊 Incremental vs Full Snapshot

| Metric | Full Snapshot | Incremental | Savings |
|--------|---------------|-------------|---------|
| **Storage/game** | 90 MB | 11 MB | **88%** ↓ |
| **Processing time** | 3-5 min | 2-3 min | **40%** ↓ |
| **Duplicates** | ~90% | 0% | **100%** ↓ |
| **Monthly cost** (100 games) | $60 | $26 | **$34** 💰 |
| **Annual savings** | — | — | **$405** 🎉 |

---

## 🚀 Quick Start

### Prerequisites

- Microsoft Fabric workspace access
- Azure CLI installed and authenticated (`az login`)
- VS Code with Fabric extension (optional)

### 1. Create Fabric Workspaces

Create three workspaces via [Fabric portal](https://app.fabric.microsoft.com):
- **MLB-Bronze** (raw data ingestion)
- **MLB-Silver** (cleaned transformations)
- **MLB-Gold** (analytics aggregations)

### 2. Create Lakehouses

In each workspace, create a lakehouse:
- MLB-Bronze → `mlb_raw_playbyplay`
- MLB-Silver → `mlb_clean_playbyplay`
- MLB-Gold → `mlb_analytics`

### 3. Upload Notebooks

Upload notebooks from `fabric-notebooks/` to respective workspaces:
- `ingest_mlb_playbyplay_incremental.py` → MLB-Bronze
- `transform_bronze_to_silver_incremental.py` → MLB-Silver
- `aggregate_silver_to_gold.py` → MLB-Gold

### 4. Create Pipeline

Use `fabric-pipeline-definition-incremental.json` to create the orchestration pipeline. See [PIPELINE-SETUP-GUIDE.md](PIPELINE-SETUP-GUIDE.md) for detailed instructions.

### 5. Test Run

Manually trigger the pipeline during an active game day to verify setup.

**📘 Complete guide**: [INCREMENTAL-IMPLEMENTATION-SUMMARY.md](INCREMENTAL-IMPLEMENTATION-SUMMARY.md)

---

## 📁 Repository Structure

### 🎯 Incremental Implementation (Recommended)

```
fabric-notebooks/
├── ingest_mlb_playbyplay_incremental.py          # Bronze: Checkpoint-based
├── transform_bronze_to_silver_incremental.py     # Silver: Incremental-aware
└── aggregate_silver_to_gold.py                   # Gold: Analytics

fabric-pipeline-definition-incremental.json       # Pipeline orchestration
fabric-pipeline-trigger.json                      # Schedule: every 5 min
```

### 📚 Documentation

| File | Purpose |
|------|---------|
| [INCREMENTAL-IMPLEMENTATION-SUMMARY.md](INCREMENTAL-IMPLEMENTATION-SUMMARY.md) | **Start here!** 30-min quick start |
| [INCREMENTAL-INGESTION-GUIDE.md](INCREMENTAL-INGESTION-GUIDE.md) | Technical deep-dive and troubleshooting |
| [INCREMENTAL-VS-FULL-COMPARISON.md](INCREMENTAL-VS-FULL-COMPARISON.md) | Visual comparison and decision guide |
| [PIPELINE-SETUP-GUIDE.md](PIPELINE-SETUP-GUIDE.md) | Pipeline deployment instructions |
| [fabric-migration-guide.md](fabric-migration-guide.md) | Azure to Fabric migration |
| [FIXES-APPLIED.md](FIXES-APPLIED.md) | Troubleshooting history |

### 📦 Full Snapshot Implementation (Reference)

```
fabric-notebooks/
├── ingest_mlb_playbyplay.py               # Bronze: Full game state
├── transform_bronze_to_silver.py          # Silver: Standard transform
└── diagnostic_test.py                     # Troubleshooting tool

fabric-pipeline-definition.json            # Full snapshot pipeline
```

### 🗂️ Legacy (Azure Data Factory)

For migration reference:
- `adf arm template.json` - ADF pipeline definition
- `pipeline orchestrator.py` - Python standalone version
- `schema.sql` - Azure SQL schema

---

## 💡 How It Works

### Incremental Ingestion (Bronze)

1. **Check checkpoint**: Load last processed play index from `game_checkpoints` table
2. **Fetch new data**: Call MLB API for current game state
3. **Extract delta**: Only capture plays beyond last checkpoint
4. **Save incremental**: Store new plays in JSON format
5. **Update checkpoint**: MERGE new state into checkpoint table

### Transformation (Silver)

1. **Read all files**: Process both full and incremental JSON files
2. **Flatten structure**: Extract play details from nested JSON
3. **Deduplicate**: MERGE on unique `play_id` (game_pk + at_bat + play_index)
4. **Write Delta**: Save to managed Delta Lake table

### Aggregation (Gold)

1. **Read Silver plays**: Access cleaned Delta table
2. **Create views**: Live scoreboard, recent plays, scoring events
3. **Calculate stats**: Batter performance, pitcher metrics
4. **Optimize**: Partition and index for Power BI

---

## 📊 Data Source

**MLB Stats API** — Free, no authentication required

- **Base URL**: `https://statsapi.mlb.com/api/v1`
- **Schedule**: `GET /schedule?sportId=1&date=YYYY-MM-DD`
- **Live Feed**: `GET /v1.1/game/{game_pk}/feed/live`
- **Documentation**: https://statsapi.mlb.com/docs

---

## 🎯 Use Cases

**Real-time Dashboards**
- Live game scores and play-by-play updates
- Recent plays feed for commentary
- Player performance tracking

**Historical Analysis**
- Season-long play patterns
- Pitcher vs batter matchups
- Scoring trends by inning

**Predictive Analytics**
- Win probability models
- Player performance forecasting
- Game outcome predictions

---

## 🔧 Configuration

### Pipeline Schedule

Default: **Every 5 minutes, 12 PM - 2 AM Eastern**

Modify `fabric-pipeline-trigger.json`:
```json
{
  "schedule": {
    "frequency": "Minute",
    "interval": 5,
    "startTime": "12:00:00",
    "endTime": "02:00:00",
    "timeZone": "Eastern Standard Time"
  }
}
```

### Checkpoint Reset

Reset specific game checkpoint:
```python
spark.sql("DELETE FROM game_checkpoints WHERE game_pk = 824278")
```

Reset all checkpoints:
```python
spark.sql("TRUNCATE TABLE game_checkpoints")
```

---

## 🐛 Troubleshooting

### Checkpoint Out of Sync
**Symptom**: Incremental notebook reports "no new plays" but game is active  
**Solution**: Reset checkpoint for that game (see Configuration above)

### Missing Plays in Silver
**Symptom**: Play count doesn't match MLB.com  
**Solution**: Check Bronze files exist, verify Silver MERGE logic, reset checkpoint if needed

### Pipeline Timeout
**Symptom**: Bronze notebook times out on first run  
**Solution**: First run bootstraps all games (acts like full snapshot). Increase timeout to 30 minutes.

**More help**: See [INCREMENTAL-INGESTION-GUIDE.md](INCREMENTAL-INGESTION-GUIDE.md) § Troubleshooting

---

## 📈 Performance

### Typical 3-Hour Game (36 runs at 5-min intervals)

**Full Snapshot Approach:**
- 36 files × 2.5 MB = 90 MB
- 6,804 plays fetched (6,615 duplicates!)
- 108-180 minutes cumulative processing

**Incremental Approach:**
- 36 files × 0.3 MB avg = 10.8 MB
- 189 plays fetched (0 duplicates!)
- 72-108 minutes cumulative processing

**Result**: 88% less storage, 40% faster processing

---

## 🤝 Contributing

Contributions welcome! Areas for enhancement:

- Additional Gold layer aggregations
- Power BI report templates
- Automated data quality checks
- ML model integration
- Multi-sport support

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

---

## 🙏 Acknowledgments

- **MLB Stats API** for free, comprehensive play-by-play data
- **Microsoft Fabric** for unified analytics platform
- **Delta Lake** for reliable data lake storage

---

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/Datamazin/mlb-play-by-play-dashboard/issues)
- **Documentation**: Start with [INCREMENTAL-IMPLEMENTATION-SUMMARY.md](INCREMENTAL-IMPLEMENTATION-SUMMARY.md)
- **Fabric Docs**: https://learn.microsoft.com/en-us/fabric/

---

**⭐ Star this repo if you find it useful!**
