#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./run_pipeline.sh <logical_query_name>"
  echo "Supported queries: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
  exit 1
fi

LOGICAL_QUERY=$1
RUN_ID=$(date +%Y%m%d_%H%M%S)

case $LOGICAL_QUERY in
  top_callers)
    DAG_ID="top_callers_by_spend_dag"
    ;;
  tower_heatmap)
    DAG_ID="tower_utilization_heatmap_dag"
    ;;
  anomalous_calls)
    DAG_ID="anomalous_call_detection_dag"
    ;;
  revenue_recon)
    DAG_ID="revenue_reconciliation_dag"
    ;;
  *)
    echo "Unknown logical query: $LOGICAL_QUERY"
    exit 1
    ;;
esac

echo "Triggering DAG: $DAG_ID with RUN_ID: $RUN_ID"

docker exec airflow airflow dags trigger -r $RUN_ID --conf "{\"run_id\":\"$RUN_ID\"}" $DAG_ID

echo "DAG triggered successfully. Check Airflow UI for progress."
