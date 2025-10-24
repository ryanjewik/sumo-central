from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import importlib.util
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import json
import os

@task
def start_msg():
    print("DAG starting")


@task
def end_msg():
    print("DAG ended")


@task.branch
def choose_job(webhook: dict):
    # For now, webhook will come from Kafka later. Use the provided webhook dict.
    webhook_type = (webhook.get("type") or "").strip()
    mapping = {
        "newBasho": "run_new_basho",
        "endBasho": "run_end_basho",
        "newMatches": "run_new_matches",
        "matchResults": "run_match_results",
    }
    return mapping.get(webhook_type, "skip_jobs")


def _load_and_call(path: str, func_name: str, webhook: dict):
    """Dynamically load a module from `path` and call `func_name(webhook)` if present."""
    spec = importlib.util.spec_from_file_location(func_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    func = getattr(module, func_name, None)
    if callable(func):
        return func(webhook)
    return None


# Helper callables for PythonOperator
def call_new_basho(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/newBasho.py", "process_new_basho", webhook)


def call_end_basho(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/endBasho.py", "process_end_basho", webhook)


def call_new_matches(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/newMatches.py", "process_new_matches", webhook)


def call_match_results(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/matchResults.py", "process_match_results", webhook)


def call_skip(webhook: dict):
    print("No matching webhook type; skipping jobs")
    return None


default_args = {"retries": 0, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="spark_local_test",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["smoke"],
) as dag:


    spark_smoke = SparkSubmitOperator(
        task_id="spark_smoke",
        application="/opt/airflow/jobs/spark_smoke.py",   # this path matches your compose mounts
        conn_id="spark_default",                          # optional; keep if you want (binary is in PATH)
        # optional tuning:
        # driver_memory="1g", executor_memory="1g", executor_cores=1,
        # verbose=True,
    )
    
    #1. postgres updates (can be done concurrently)
    #2. mongo updates (can be done concurrently)
    #3. bronze to silver
    #4. silver to gold
    #5. update ML training set
    #6. trigger model retraining


    # Use PythonOperator to call job functions. Wrappers are defined at module level.

    # Join operator continues once one upstream succeeds (branching will SKIP the others)
    join = EmptyOperator(task_id="join", trigger_rule="one_success")

    # end task as TaskFlow
    end_task = end_msg()

    # For now pass a fixed sample webhook dict; replace with Kafka/XCom later
    webhook = {
        "received_at": 1756357624,
        "type": "newBasho",
        "headers": {
            "Host": "74de6cbafcff.ngrok-free.app",
            "User-Agent": "Go-http-client/2.0",
            "Content-Length": "216",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json",
            "X-Forwarded-For": "5.78.73.189",
            "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
            "X-Forwarded-Proto": "https",
            "X-Webhook-Signature": "51d5e8f38a6624f8ff413419328855ad419c79f4d183c0544f2441a592895533"
        },
        "raw": {
            "type": "newBasho",
            "payload": "eyJkYXRlIjoiMjAyMzExIiwibG9jYXRpb24iOiJGdWt1b2thLCBGdWt1b2thIEludGVybmF0aW9uYWwgQ2VudGVyIiwic3RhcnREYXRlIjoiMjAyMy0xMS0xMlQwMDowMDowMFoiLCJlbmREYXRlIjoiMjAyMy0xMS0yNlQwMDowMDowMFoifQ=="
        },
        "payload_decoded": {
            "date": "202311",
            "location": "Fukuoka, Fukuoka International Center",
            "startDate": "2023-11-12T00:00:00Z",
            "endDate": "2023-11-26T00:00:00Z"
        }
    }

    # wiring: start -> spark_smoke -> branch -> chosen job -> join -> end
    start_task = start_msg()
    spark_smoke  # operator already defined above
    branch_task = choose_job(webhook)

    # create PythonOperator tasks and pass the webhook via op_kwargs
    new_basho = PythonOperator(
        task_id="run_new_basho",
        python_callable=call_new_basho,
        op_kwargs={"webhook": webhook},
    )

    end_basho = PythonOperator(
        task_id="run_end_basho",
        python_callable=call_end_basho,
        op_kwargs={"webhook": webhook},
    )

    new_matches = PythonOperator(
        task_id="run_new_matches",
        python_callable=call_new_matches,
        op_kwargs={"webhook": webhook},
    )

    match_results = PythonOperator(
        task_id="run_match_results",
        python_callable=call_match_results,
        op_kwargs={"webhook": webhook},
    )

    skip_task = PythonOperator(
        task_id="skip_jobs",
        python_callable=call_skip,
        op_kwargs={"webhook": webhook},
    )

    # Connect tasks/operators
    start_task >> spark_smoke
    spark_smoke >> branch_task
    branch_task >> [new_basho, end_basho, new_matches, match_results, skip_task]
    [new_basho, end_basho, new_matches, match_results, skip_task] >> join >> end_task
