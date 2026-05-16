# Fabric Data Pipeline Setup Guide

## 📋 Pipeline Overview

The **MLB_PlayByPlay_Pipeline** orchestrates the complete data flow:

```
Bronze (Ingest) → Silver (Transform) → Gold (Aggregate)
     ↓                  ↓                    ↓
  Raw JSON        Clean Delta          Analytics
  Files/raw/      Tables/plays         Tables/scoreboard
```

## 🎯 Pipeline Features

✅ **Sequential Execution** - Silver waits for Bronze, Gold waits for Silver  
✅ **Error Handling** - Each activity retries 2 times with 30-second intervals  
✅ **Parameterized** - Uses current date for filtering  
✅ **Timeout Protection** - 30-minute max per activity  
✅ **Status Tracking** - Variables track pipeline state  

## 📂 Files Created

- **fabric-pipeline-definition.json** - Main pipeline definition
- **fabric-pipeline-trigger.json** - Schedule trigger (every 5 minutes)

---

## 🚀 Option 1: Create Pipeline via Fabric Portal (Recommended)

### Step 1: Create Pipeline

1. Go to **MLB-Bronze workspace** in Fabric Portal
2. Click **+ New** → **Data Pipeline**
3. Name it: **MLB_PlayByPlay_Pipeline**
4. Click **Create**

### Step 2: Add Activities

**Activity 1: Bronze Notebook**
1. Drag **Notebook** activity to canvas
2. Name: `Bronze_Ingest_MLB_Data`
3. **Settings** tab:
   - Workspace: MLB-Bronze
   - Notebook: ingest_mlb_playbyplay
   - Lakehouse: mlb_raw_playbyplay
4. **General** tab:
   - Timeout: 30 minutes
   - Retry: 2
   - Retry interval: 30 seconds

**Activity 2: Silver Notebook**
1. Drag another **Notebook** activity
2. Name: `Silver_Transform_Data`
3. **Connect** Bronze activity's success output to this activity
4. **Settings** tab:
   - Workspace: MLB-Silver
   - Notebook: transform_bronze_to_silver
   - Lakehouse: mlb_clean_playbyplay
   - Parameters: Add parameter `game_date` = `@formatDateTime(utcnow(), 'yyyy-MM-dd')`
5. **General** tab: Same timeout/retry settings

**Activity 3: Gold Notebook**
1. Drag third **Notebook** activity
2. Name: `Gold_Create_Analytics`
3. **Connect** Silver activity's success output to this activity
4. **Settings** tab:
   - Workspace: MLB-Gold
   - Notebook: aggregate_silver_to_gold
   - Lakehouse: mlb_analytics
   - Parameters: Add parameter `game_date` = `@formatDateTime(utcnow(), 'yyyy-MM-dd')`
5. **General** tab: Same timeout/retry settings

### Step 3: Add Schedule Trigger

1. In pipeline editor, click **Home** → **Add trigger** → **New**
2. Configure:
   - Name: `MLB_PlayByPlay_Schedule_Trigger`
   - Type: **Schedule**
   - Recurrence: **Every 5 minutes**
   - Start date: Today
   - Time zone: **Eastern Time (US & Canada)**
   - Advanced schedule (optional):
     - Hours: 12 PM - 2 AM (active game hours)
     - Days: All days
3. Click **Save**

### Step 4: Validate & Publish

1. Click **Validate** button → Check for errors
2. Click **Save** to save pipeline
3. Click **Publish** to make it live
4. Trigger will activate automatically

---

## 🚀 Option 2: Create Pipeline via REST API

Use the JSON definitions to create the pipeline programmatically:

```powershell
# Get authentication token
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{ 
    Authorization = "Bearer $token"
    "Content-Type" = "application/json"
}

# Create Pipeline
$pipelineJson = Get-Content "fabric-pipeline-definition.json" -Raw
$workspaceId = "903342ad-66da-4319-8f56-9ad0602d0aa7"  # MLB-Bronze

Invoke-RestMethod `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items" `
    -Method Post `
    -Headers $headers `
    -Body $pipelineJson

Write-Host "✅ Pipeline created successfully!"
```

---

## 🧪 Testing the Pipeline

### Manual Test Run

1. Open pipeline in Fabric Portal
2. Click **Run** → **Run now**
3. Monitor execution in **Output** tab
4. Check each activity's status and logs

### Expected Results

**Bronze Activity:**
```
Status: Succeeded
Output: {"status": "success", "active_games": 1, "files_saved": 1}
Duration: ~30 seconds
```

**Silver Activity:**
```
Status: Succeeded
Output: {"status": "success", "game_date": "2026-05-16", "plays_processed": 59}
Duration: ~1-2 minutes
```

**Gold Activity:**
```
Status: Succeeded
Output: {"status": "success", "games": 1, "total_plays": 59, "scoring_plays": 5}
Duration: ~1-2 minutes
```

### Verify Data

**Check Bronze Files:**
```
MLB-Bronze → mlb_raw_playbyplay → Files → raw → 2026-05-16 → {game_pk} → {timestamp}.json
```

**Check Silver Tables:**
```
MLB-Silver → mlb_clean_playbyplay → Tables → plays
Row count: 59+
```

**Check Gold Tables:**
```
MLB-Gold → mlb_analytics → Tables
- live_scoreboard (1 row)
- recent_plays (10 rows)
- scoring_plays (5+ rows)
- batter_statistics (15+ rows)
```

---

## 📊 Monitoring

### View Pipeline Runs

1. Go to MLB-Bronze workspace
2. Click **Monitor** → **Pipeline runs**
3. See all executions with status and duration

### Check Activity Details

1. Click any pipeline run
2. See individual activity status
3. Click activity → **Output** to see notebook results
4. Click **Input** to see parameters passed

### Set Up Alerts (Optional)

1. Pipeline settings → **Alerts**
2. Configure:
   - Alert on failure
   - Email notifications
   - Webhook to Teams/Slack

---

## 🔧 Troubleshooting

### Pipeline Fails at Bronze

**Issue:** No active games found  
**Solution:** Normal - pipeline will exit gracefully with no data

**Issue:** API timeout  
**Solution:** Increase timeout in activity settings

### Pipeline Fails at Silver

**Issue:** Cannot read Bronze files  
**Solution:** Check Bronze files exist in Files/raw folder

**Issue:** JSON parsing error  
**Solution:** Check MLB API response format hasn't changed

### Pipeline Fails at Gold

**Issue:** Cannot read Silver table  
**Solution:** Verify Silver notebook ran successfully and created `plays` table

**Issue:** Session timeout  
**Solution:** Reduce aggregation complexity or increase Spark resources

---

## ⚙️ Configuration Options

### Adjust Schedule

**More Frequent (Every 2 minutes):**
```json
"recurrence": {
  "frequency": "Minute",
  "interval": 2
}
```

**Only During Games (12 PM - 2 AM ET):**
```json
"schedule": {
  "hours": [12,13,14,15,16,17,18,19,20,21,22,23,0,1,2]
}
```

**Specific Days Only:**
```json
"schedule": {
  "weekDays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
}
```

### Add Notifications

Add a **Send Email** activity after Gold:
1. Drag **Send Email** activity
2. Connect from Gold success
3. Configure recipients and message:
   ```
   Subject: MLB Pipeline Success - @{formatDateTime(utcnow(), 'yyyy-MM-dd HH:mm')}
   Body: Pipeline completed successfully!
         Games processed: @{activity('Gold_Create_Analytics').output.games}
         Total plays: @{activity('Gold_Create_Analytics').output.total_plays}
   ```

---

## 📈 Performance Optimization

### Parallel Execution (Advanced)

If you have multiple independent game files:
1. Add **ForEach** activity after Bronze
2. Iterate over game_pk list
3. Run Silver/Gold in parallel for each game

### Incremental Processing

Modify notebooks to only process new files:
- Track last processed timestamp
- Filter files by modification time
- Skip already-processed games

### Resource Scaling

Adjust Spark pool settings:
- Increase executors for faster processing
- Use autoscaling for variable loads
- Schedule scale-up during peak hours

---

## 🎯 Next Steps

1. ✅ Create pipeline in Fabric Portal
2. ✅ Run manual test
3. ✅ Enable schedule trigger
4. ✅ Monitor first few automated runs
5. ✅ Create Power BI report on Gold tables
6. ✅ Set up alerts and notifications

---

## 📚 Related Files

- [fabric-pipeline-definition.json](fabric-pipeline-definition.json) - Main pipeline JSON
- [fabric-pipeline-trigger.json](fabric-pipeline-trigger.json) - Schedule trigger JSON
- [fabric-notebooks/](fabric-notebooks/) - All notebook code
- [fabric-migration-guide.md](fabric-migration-guide.md) - Architecture overview
- [FIXES-APPLIED.md](FIXES-APPLIED.md) - Path and import fixes

---

## 🆘 Support

If you encounter issues:
1. Run **diagnostic_test.py** notebook first
2. Check activity logs in pipeline runs
3. Verify lakehouse attachments
4. Review OneLake paths in notebooks

Need help? Check the troubleshooting section or review the fix documentation! 🚀
