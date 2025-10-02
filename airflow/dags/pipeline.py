from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_local_test",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
):
    SparkSubmitOperator(
        task_id="spark_smoke",
        application="/opt/airflow/dags/jobs/spark_smoke.py",
        conn_id="spark_default",   # 👈 use the connection, not master=
        conf={"spark.ui.showConsoleProgress": "true"},
        # spark_binary could also be passed here, but we set it in the connection extra
    )
