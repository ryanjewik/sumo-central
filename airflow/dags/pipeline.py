from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="spark_local_test",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    spark_smoke = SparkSubmitOperator(
        task_id="spark_smoke",
        application="/opt/airflow/dags/jobs/spark_smoke.py",
        conn_id="spark_default",
        conf={"spark.ui.showConsoleProgress": "true"},
        verbose=True,
    )
