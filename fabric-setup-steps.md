# MLB Play-by-Play Pipeline - Fabric Setup Steps

## Step-by-Step Setup (VS Code + Fabric Portal)

### ✅ Pre-requisites Completed
- [x] Fabric extensions installed
- [x] Azure authenticated
- [x] Medallion workspaces exist (Bronze/Silver/Gold)

---

## 🚨 Issue Found: No Capacity Assigned

Your MLB-Bronze/Silver/Gold workspaces don't have Fabric capacity assigned yet. 

### Option A: Assign Existing Capacity (Recommended)

**Via Fabric Portal:**
1. Go to https://app.fabric.microsoft.com
2. Open **Workspace Settings** for each workspace (Bronze, Silver, Gold)
3. Go to **License Info** → **Assign to capacity**
4. Select an available Fabric capacity (F2 or higher)

**OR Quick Command:**
```powershell
# List available capacities
az fabric capacity list --output table

# Assign capacity to workspaces (replace <capacity-id>)
az rest --method PATCH `
  --uri "https://api.fabric.microsoft.com/v1/workspaces/903342ad-66da-4319-8f56-9ad0602d0aa7" `
  --headers "Content-Type=application/json" `
  --body '{"capacityId":"<your-capacity-id>"}'
```

### Option B: Create New Fabric Capacity

If you don't have capacity yet:
1. Go to **Azure Portal** → **Create Resource** → **Microsoft Fabric**
2. Create **Fabric Capacity** (F2 minimum - ~$262/month, can pause when not in use)
3. Return to Fabric portal and assign workspaces to this capacity

---

## 📋 Once Capacity is Assigned

### 1. Create Lakehouses via VS Code

**Method 1: VS Code Extension UI**
1. Open **Fabric** view in VS Code sidebar
2. Expand **MLB-Bronze** workspace
3. Right-click → **Create Item** → **Lakehouse**
4. Name: `mlb_raw_playbyplay`

Repeat for Silver and Gold:
- MLB-Silver: `mlb_clean_playbyplay`
- MLB-Gold: `mlb_analytics`

**Method 2: PowerShell (after capacity assigned)**
```powershell
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{ 
    Authorization = "Bearer $token"
    "Content-Type" = "application/json"
}

# Bronze Lakehouse
$body = @{ displayName = "mlb_raw_playbyplay"; type = "Lakehouse" } | ConvertTo-Json
Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/903342ad-66da-4319-8f56-9ad0602d0aa7/items" `
  -Headers $headers -Method Post -Body $body

# Silver Lakehouse
$body = @{ displayName = "mlb_clean_playbyplay"; type = "Lakehouse" } | ConvertTo-Json
Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/7ba5d5d0-00f6-4032-8791-95edc329aeda/items" `
  -Headers $headers -Method Post -Body $body

# Gold Lakehouse
$body = @{ displayName = "mlb_analytics"; type = "Lakehouse" } | ConvertTo-Json
Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/2da1d978-8713-4204-86ca-a427a4e14c7f/items" `
  -Headers $headers -Method Post -Body $body
```

---

### 2. Create Notebooks

**Bronze → Silver Notebook** (in MLB-Silver workspace):
- Name: `transform_bronze_to_silver`
- See: `notebooks/transform_bronze_to_silver.py`

**Silver → Gold Notebook** (in MLB-Gold workspace):
- Name: `aggregate_silver_to_gold`
- See: `notebooks/aggregate_silver_to_gold.py`

**Data Ingestion Notebook** (in MLB-Bronze workspace):
- Name: `ingest_mlb_playbyplay`
- See: `notebooks/ingest_mlb_playbyplay.py`

---

### 3. Create Data Pipeline

**In MLB-Bronze workspace:**
1. Create **Data Pipeline**: `MLB_PlayByPlay_Pipeline`
2. Add **Schedule Trigger**: Every 5 minutes (during game hours)
3. Activities:
   - **Notebook**: `ingest_mlb_playbyplay` (fetches MLB API)
   - **Notebook**: `transform_bronze_to_silver` (Bronze → Silver)
   - **Notebook**: `aggregate_silver_to_gold` (Silver → Gold)

---

### 4. Create Power BI Report

**In MLB-Gold workspace:**
1. Open `mlb_analytics` lakehouse
2. Click **New semantic model**
3. Select tables/views
4. Click **New report**
5. Add visuals (see migration guide)

---

## 📁 Files to Create

### Notebooks Directory Structure
```
notebooks/
├── ingest_mlb_playbyplay.py      # Bronze: Fetch MLB API → raw JSON
├── transform_bronze_to_silver.py  # Silver: Clean & transform
└── aggregate_silver_to_gold.py    # Gold: Analytics aggregations
```

### Current Project Files
```
c:\Users\metsy\dev\development\active-projects\mlb-play-by-play-dashboard\
├── pipeline orchestrator.py  → Can reuse for Bronze notebook
├── schema.sql                → Convert to Delta table schema
├── adf arm template.json     → Reference for pipeline logic
└── requirements.txt          → Use in Fabric environment
```

---

## ⚡ Quick Start Commands

### Check if capacity is now assigned:
```powershell
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{ Authorization = "Bearer $token" }
$ws = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/903342ad-66da-4319-8f56-9ad0602d0aa7" -Headers $headers
if ($ws.capacityId) { 
    Write-Host "✅ Capacity assigned: $($ws.capacityId)" -ForegroundColor Green 
} else { 
    Write-Host "❌ No capacity assigned yet" -ForegroundColor Red 
}
```

### List your Fabric capacities:
```powershell
az rest --method GET --uri "https://management.azure.com/subscriptions/a261006a-69fe-441a-bbb8-59270a4d2f01/providers/Microsoft.Fabric/capacities?api-version=2023-11-01" | ConvertFrom-Json | Select -ExpandProperty value | Format-Table -Property name, location, sku
```

---

## 🎯 Next Steps

1. **Assign Fabric capacity** to Bronze/Silver/Gold workspaces
2. **Create Lakehouses** in each workspace
3. I'll generate the **notebook code** for you
4. Set up the **Data Pipeline**
5. Create **Power BI report**

Let me know when capacity is assigned and I'll continue with the setup! 🚀
