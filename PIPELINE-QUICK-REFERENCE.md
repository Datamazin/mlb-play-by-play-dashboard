# MLB Play-by-Play Data Pipeline - Quick Reference

## 🎯 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MLB_PlayByPlay_Pipeline                         │
│                     Every 5 minutes (12 PM - 2 AM)                  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
        ┌─────────────────────────────────────────┐
        │  Bronze: Ingest MLB Data                │
        │  Notebook: ingest_mlb_playbyplay        │
        │  Workspace: MLB-Bronze                  │
        │  Lakehouse: mlb_raw_playbyplay          │
        │  Duration: ~30 seconds                  │
        │  Output: Raw JSON files                 │
        └──────────────┬──────────────────────────┘
                       │ Success
                       ▼
        ┌─────────────────────────────────────────┐
        │  Silver: Transform Data                 │
        │  Notebook: transform_bronze_to_silver   │
        │  Workspace: MLB-Silver                  │
        │  Lakehouse: mlb_clean_playbyplay        │
        │  Duration: ~1-2 minutes                 │
        │  Output: Delta table 'plays'            │
        └──────────────┬──────────────────────────┘
                       │ Success
                       ▼
        ┌─────────────────────────────────────────┐
        │  Gold: Create Analytics                 │
        │  Notebook: aggregate_silver_to_gold     │
        │  Workspace: MLB-Gold                    │
        │  Lakehouse: mlb_analytics               │
        │  Duration: ~1-2 minutes                 │
        │  Output: 4 analytics tables             │
        └──────────────┬──────────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────────┐
        │  ✅ SUCCESS - Data Ready for BI         │
        └─────────────────────────────────────────┘
```

## 📊 Gold Tables Created

| Table Name | Description | Rows (typical) |
|------------|-------------|----------------|
| **live_scoreboard** | Current game scores and status | 1-15 (per game day) |
| **recent_plays** | Last 10 plays per game | 10-150 |
| **scoring_plays** | Only plays that scored runs | 5-30 |
| **batter_statistics** | Player batting stats per game | 20-200 |

## ⚙️ Pipeline Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `RunDate` | `@formatDateTime(utcnow(), 'yyyy-MM-dd')` | Current date for filtering |
| Timeout | 30 minutes | Max time per activity |
| Retry | 2 attempts | Retries on failure |
| Retry Interval | 30 seconds | Wait between retries |

## 🕐 Schedule

**Frequency:** Every 5 minutes  
**Active Hours:** 12:00 PM - 2:00 AM ET (18 hours)  
**Days:** All days (Mon-Sun)  
**Runs per Day:** ~216 executions  
**Season:** MLB season (April - October)

## 📈 Expected Performance

| Metric | Typical Value |
|--------|---------------|
| Total pipeline duration | 3-5 minutes |
| Bronze (API fetch) | 20-40 seconds |
| Silver (transform) | 1-2 minutes |
| Gold (aggregate) | 1-2 minutes |
| Data latency | ~5-10 minutes behind live |

## 🎮 Quick Commands

### Create Pipeline via Portal
```
1. MLB-Bronze workspace → + New → Data Pipeline
2. Name: MLB_PlayByPlay_Pipeline
3. Add 3 Notebook activities (see guide)
4. Add Schedule Trigger (every 5 min)
5. Validate → Save → Publish
```

### Create Pipeline via API
```powershell
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$pipeline = Get-Content "fabric-pipeline-definition.json" -Raw
Invoke-RestMethod `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/903342ad-66da-4319-8f56-9ad0602d0aa7/items" `
    -Method Post -Headers $headers -Body $pipeline
```

### Test Pipeline
```
1. Open pipeline in Fabric Portal
2. Click "Run" → "Run now"
3. Monitor in "Output" tab
4. Verify data in each lakehouse
```

### Check Status
```powershell
# List pipeline runs
$runs = Invoke-RestMethod `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/903342ad-66da-4319-8f56-9ad0602d0aa7/pipelines/{pipeline-id}/pipelineruns" `
    -Headers $headers

# Show recent runs
$runs.value | Select-Object runId, status, runStart, runEnd, durationInMs | Format-Table
```

## 🚨 Common Issues & Fixes

| Issue | Solution |
|-------|----------|
| Bronze: No active games | Normal - pipeline exits gracefully |
| Silver: Cannot read Bronze | Check Files/raw folder has JSON files |
| Gold: Cannot read Silver | Run diagnostic_test.py notebook |
| Kernel crash | Restart kernel, check memory limits |
| Timeout | Increase timeout setting or optimize notebook |

## 📁 Project Files

```
mlb-play-by-play-dashboard/
├── fabric-pipeline-definition.json      ← Pipeline JSON
├── fabric-pipeline-trigger.json         ← Schedule JSON
├── PIPELINE-SETUP-GUIDE.md              ← Detailed instructions
├── PIPELINE-QUICK-REFERENCE.md          ← This file
├── fabric-notebooks/
│   ├── ingest_mlb_playbyplay.py         ← Bronze notebook
│   ├── transform_bronze_to_silver.py    ← Silver notebook
│   ├── aggregate_silver_to_gold.py      ← Gold notebook
│   └── diagnostic_test.py               ← Troubleshooting
├── fabric-migration-guide.md            ← Architecture guide
├── FIXES-APPLIED.md                     ← Bug fixes log
└── fabric-setup-steps.md                ← Initial setup
```

## 🎯 Success Criteria

✅ Bronze creates JSON files in Files/raw/YYYY-MM-DD/{game_pk}/  
✅ Silver creates 'plays' table with all game plays  
✅ Gold creates 4 analytics tables with aggregations  
✅ Pipeline completes in < 5 minutes  
✅ Schedule runs every 5 minutes during game hours  
✅ Power BI reports refresh automatically  

## 🆘 Need Help?

1. **Detailed Setup:** See [PIPELINE-SETUP-GUIDE.md](PIPELINE-SETUP-GUIDE.md)
2. **Architecture:** See [fabric-migration-guide.md](fabric-migration-guide.md)
3. **Troubleshooting:** Run [diagnostic_test.py](fabric-notebooks/diagnostic_test.py)
4. **Bug Fixes:** Review [FIXES-APPLIED.md](FIXES-APPLIED.md)

## 🚀 Next: Create Power BI Report

Once pipeline is running:
1. Open **MLB-Gold** workspace
2. Select **mlb_analytics** lakehouse
3. Click **New semantic model**
4. Choose tables (live_scoreboard, recent_plays, etc.)
5. Click **New report**
6. Build dashboard with:
   - Live scoreboard cards
   - Recent plays table
   - Scoring timeline chart
   - Player statistics matrix

---

**Status:** ✅ Pipeline definition complete - Ready to deploy!
