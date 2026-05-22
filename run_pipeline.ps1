param(
    [Parameter(Mandatory = $true)]
    [string]$LogicalQuery
)

$runId = Get-Date -Format "yyyyMMdd_HHmmss"

switch ($LogicalQuery) {
    "top_callers" {
        $dagId = "top_callers_by_spend_dag"
    }
    "tower_heatmap" {
        $dagId = "tower_utilization_heatmap_dag"
    }
    "anomalous_calls" {
        $dagId = "anomalous_call_detection_dag"
    }
    "revenue_recon" {
        $dagId = "revenue_reconciliation_dag"
    }
    default {
        Write-Host "Unknown logical query: $LogicalQuery"
        Write-Host "Supported queries: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
        exit 1
    }
}

Write-Host "Triggering DAG: $dagId with RUN_ID: $runId"
docker exec airflow airflow dags unpause $dagId
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

docker exec airflow airflow dags trigger $dagId -r $runId

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "DAG triggered successfully. Check Airflow UI for progress."
