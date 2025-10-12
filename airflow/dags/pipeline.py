from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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

    from airflow.operators.bash import BashOperator

    spark_smoke = BashOperator(
        task_id="spark_smoke",
        bash_command=r"""
            set -euo pipefail
        # Find the spark-master container via its compose service label
        MASTER_ID="$(docker ps -q -f label=com.docker.compose.service=spark-master)"
        if [ -z "$MASTER_ID" ]; then
        echo 'spark-master container not found (is the stack up?)'
        docker ps -a
        exit 1
        fi

        # Submit inside the master container
        docker exec "$MASTER_ID" sh -lc '/opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/jobs/spark_smoke.py'
        """,
        )


    end = PythonOperator(
        task_id="end",
        python_callable=end_msg,
    )

    start >> spark_smoke >> end
