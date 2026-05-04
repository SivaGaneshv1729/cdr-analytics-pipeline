from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'tower_utilization_heatmap_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    submit_job = BashOperator(
        task_id='submit_spark_job',
        bash_command='spark-submit --master spark://spark-master:7077 /jobs/tower_heatmap.py --run_id {{ dag_run.conf.get("run_id", "default_run") }}'
    )
