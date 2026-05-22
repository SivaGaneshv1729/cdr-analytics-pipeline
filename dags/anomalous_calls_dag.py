from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'anomalous_call_detection_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    submit_job = BashOperator(
        task_id='submit_spark_job',
        bash_command='spark-submit --master spark://spark-master:7077 /jobs/anomalous_calls.py --run_id {{ dag_run.conf.get("run_id", dag_run.run_id) if dag_run.conf else dag_run.run_id }}'
    )
