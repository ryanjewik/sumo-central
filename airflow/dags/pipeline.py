from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

def start_msg():
    print("DAG starting")

def end_msg():
    print("DAG ended")


default_args = {"retries": 0, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="spark_local_test",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["smoke"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_msg,
    )

    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    spark_smoke = SparkSubmitOperator(
        task_id="spark_smoke",
        application="/opt/airflow/jobs/spark_smoke.py",   # this path matches your compose mounts
        conn_id="spark_default",                          # optional; keep if you want (binary is in PATH)
        # optional tuning:
        # driver_memory="1g", executor_memory="1g", executor_cores=1,
        # verbose=True,
    )



    end = PythonOperator(
        task_id="end",
        python_callable=end_msg,
    )

    start >> spark_smoke >> end
