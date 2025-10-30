from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import importlib.util
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
# Local helper to keep the DAG small and reusable
from mongo_utils import sanitize_mongo_uri
import json
import os
import sys
import logging

@task
def start_msg():
    print("DAG starting")


@task
def end_msg():
    print("DAG ended")


@task.branch
def choose_job(webhook: dict, db_type: str):
    webhook_type = (webhook.get("type") or "").strip()
    mapping = {
        "newBasho": f"run_new_basho_{db_type}",
        "endBasho": f"run_end_basho_{db_type}",
        "newMatches": f"run_new_matches_{db_type}",
        "matchResults": f"run_match_results_{db_type}",
    }

    return mapping.get(webhook_type, "skip_jobs")


def _load_and_call(path: str, func_name: str, webhook: dict):
    jobs_dir = os.path.dirname(path)
    if jobs_dir and jobs_dir not in sys.path:
        sys.path.insert(0, jobs_dir)

    spec = importlib.util.spec_from_file_location(func_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    func = getattr(module, func_name, None)
    if callable(func):
        return func(webhook)
    return None


# Helper callables for PythonOperator
def call_new_basho_postgres(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/postgresNewBasho.py", "process_new_basho", webhook)


def call_end_basho_postgres(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/postgresEndBasho.py", "process_end_basho", webhook)


def call_new_matches_postgres(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/postgresNewMatches.py", "process_new_matches", webhook)


def call_match_results_postgres(webhook: dict):
    return _load_and_call("/opt/airflow/jobs/postgresMatchResults.py", "process_match_results", webhook)


def call_skip(webhook: dict):
    print("No matching webhook type; skipping jobs")
    return None


def call_homepage(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/spark_homepage.py", "run_homepage_job", webhook)


def call_new_matches_mongo(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/mongoNewMatches.py", "process_new_matches", webhook)

def call_new_basho_mongo(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/mongoNewBasho.py", "process_new_basho", webhook)

def call_end_basho_mongo(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/mongoEndBasho.py", "process_end_basho", webhook)

def call_match_results_mongo(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/spark_mongoMatchResults.py", "process_match_results", webhook)


def call_push_webhook_xcom(webhook: dict):
  """Return webhook JSON string to be pushed to XCom for SparkSubmitOperator."""
  return json.dumps(webhook)


def call_data_cleaning_spark(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/spark_data_cleaning.py", "process_data", webhook)

def call_ml_dataset_spark(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/spark_ml_dataset.py", "process_ml_dataset", webhook)

def call_ml_training_spark(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/spark_ml_training.py", "process_ml_training", webhook)

default_args = {"retries": 0, "retry_delay": timedelta(minutes=1)}

with DAG(
  dag_id="spark_local_test",
  start_date=datetime(2025, 9, 1),
  schedule=None,
  catchup=False,
  default_args=default_args,
  tags=["smoke"],
) as dag:
  
  
  @task
  def spark_conf():
      """
      Build a minimal Spark conf for the rest of the DAG:
      - resolve Postgres
      - resolve Mongo
      - expose S3_SMOKE_PATH
      NOTE: we NO LONGER push AWS creds via XCom/Jinja — containers already have them.
      """
      logger = logging.getLogger("pipeline.spark_conf")

      # --- Postgres via Airflow connection ---
      try:
          sumo_conn = BaseHook.get_connection("sumo_db")
          conn_host = sumo_conn.host
          conn_port = sumo_conn.port or 5432
          conn_db = sumo_conn.schema
          if not conn_host or not conn_db:
              raise RuntimeError("Airflow connection 'sumo_db' must include host and schema (database name)")
          jdbc_url = f"jdbc:postgresql://{conn_host}:{conn_port}/{conn_db}"
          db_username = sumo_conn.login
          db_password = sumo_conn.password
      except Exception as e:
          raise RuntimeError(
              "Failed to build JDBC URL from Airflow connection 'sumo_db'. "
              "Ensure the connection exists and has host/schema set"
          ) from e

      # --- Mongo via Airflow connection (or env fallback) ---
      mongo_uri = None
      mongo_db_name = None
      extras = {}
      host = None
      try:
          mconn = BaseHook.get_connection("mongo_default")
          try:
              mongo_uri = mconn.get_uri()
          except Exception:
              if not mconn.host:
                  raise RuntimeError("Airflow connection 'mongo_default' must include a host or URI")
              if mconn.login and mconn.password:
                  mongo_uri = f"mongodb://{mconn.login}:{mconn.password}@{mconn.host}:{mconn.port or 27017}/{mconn.schema or ''}"
              else:
                  mongo_uri = f"mongodb://{mconn.host}:{mconn.port or 27017}/{mconn.schema or ''}"
          host = mconn.host
          try:
              extras = mconn.extra_dejson or {}
          except Exception:
              extras = {}
          mongo_db_name = mconn.schema
          logger.info("Using Airflow connection 'mongo_default' for Mongo URI and DB name")
      except Exception:
          # fallback to env
          mongo_uri = os.environ.get("MONGO_URI")
          mongo_db_name = os.environ.get("MONGO_DB_NAME")
          extras = {}
          host = None
          logger.info("Airflow connection 'mongo_default' not found; using env vars")

      if not mongo_uri:
          raise RuntimeError("MongoDB URI not available")

      # import here to avoid circulars
      from mongo_utils import sanitize_mongo_uri  # type: ignore
      safe_uri = sanitize_mongo_uri(mongo_uri, host=host, extras=extras)

      if not mongo_db_name:
          raise RuntimeError("MongoDB name not found")

      # --- S3 smoke path (leave this; executor needs it) ---
      try:
          s3_path = Variable.get("S3_SMOKE_PATH", default_var=None)
      except Exception:
          s3_path = None
      s3_path = s3_path or os.environ.get("S3_SMOKE_PATH")

      # build the minimal conf we hand to downstream SparkSubmitOperators
      homepage_conf = {
          "spark.pyspark.python": "python3",
          "spark.executorEnv.PYSPARK_PYTHON": "python3",
          "spark.pyspark.driver.python": "python3",
          "spark.jars": "/opt/spark/jars/postgresql-42.6.0.jar",
          # Mongo
          "spark.executorEnv.MONGO_URI": safe_uri,
          "spark.executorEnv.MONGO_DB_NAME": mongo_db_name,
          "spark.executorEnv.MONGO_COLL_NAME": "homepage",
          "spark.driverEnv.MONGO_URI": safe_uri,
          "spark.driverEnv.MONGO_DB_NAME": mongo_db_name,
          "spark.driverEnv.MONGO_COLL_NAME": "homepage",
          # Postgres (executor + driver)
          "spark.executorEnv.DB_HOST": conn_host,
          "spark.executorEnv.DB_PORT": str(conn_port),
          "spark.executorEnv.DB_NAME": conn_db,
          "spark.driverEnv.DB_HOST": conn_host,
          "spark.driverEnv.DB_PORT": str(conn_port),
          "spark.driverEnv.DB_NAME": conn_db,
          # S3 smoke path (driver + executor)
          "spark.executorEnv.S3_SMOKE_PATH": s3_path,
          "spark.driverEnv.S3_SMOKE_PATH": s3_path,
      }

      if db_username:
          homepage_conf["spark.executorEnv.DB_USERNAME"] = db_username
          homepage_conf["spark.driverEnv.DB_USERNAME"] = db_username
      if db_password:
          homepage_conf["spark.executorEnv.DB_PASSWORD"] = db_password
          homepage_conf["spark.driverEnv.DB_PASSWORD"] = db_password

      # NOTE: we deliberately DO NOT inject AWS creds or AWS region here.
      # containers (docker-compose) already export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION=us-west-2
      # and spark_smoke was failing because Jinja was overriding those with literal template strings.

      return {
          "conf": homepage_conf,
          "jdbc_url": jdbc_url,
          "mongo_uri": safe_uri,
          "mongo_db": mongo_db_name,
          "mongo_coll": "homepage",
      }


  spark_smoke = SparkSubmitOperator(
        task_id="spark_smoke",
        application="/opt/airflow/jobs/spark_smoke.py",
        conn_id="spark_default",
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=2,
        conf={
          # base python stuff
          "spark.pyspark.python": "python3",
          "spark.executorEnv.PYSPARK_PYTHON": "python3",
          "spark.pyspark.driver.python": "python3",
          "spark.jars": "/opt/spark/jars/postgresql-42.6.0.jar",

          # --- Mongo from spark_conf ---
          "spark.executorEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_URI'] }}",
          "spark.executorEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_DB_NAME'] }}",
          "spark.executorEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_COLL_NAME'] }}",

          # --- Postgres from spark_conf ---
          "spark.executorEnv.DB_HOST": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_HOST'] }}",
          "spark.executorEnv.DB_PORT": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_PORT'] }}",
          "spark.executorEnv.DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_NAME'] }}",
          "spark.executorEnv.DB_USERNAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf'].get('spark.executorEnv.DB_USERNAME','') }}",
          "spark.executorEnv.DB_PASSWORD": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf'].get('spark.executorEnv.DB_PASSWORD','') }}",

          # --- S3 path only ---
          "spark.executorEnv.S3_SMOKE_PATH": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.S3_SMOKE_PATH'] }}",
          "spark.driverEnv.S3_SMOKE_PATH": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.S3_SMOKE_PATH'] }}",

          # --- MINIMAL AWS (no Jinja) ---
          # read from Airflow container env at DAG-parse/runtime
          "spark.executorEnv.AWS_REGION": os.environ.get("AWS_REGION", "us-west-2"),
          "spark.executorEnv.AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
          "spark.executorEnv.AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        },
        # no need to push xcom from spark job
        do_xcom_push=False,
    )
    
  #2. mongo updates (can be done concurrently)
  #3. bronze to silver
  #4. silver to gold
  #5. update ML training set
  #6. trigger model retraining

  webhook = {
  "received_at": 1756357624,
  "type": "matchResults",
  "headers": {
    "Host": "74de6cbafcff.ngrok-free.app",
    "User-Agent": "Go-http-client/2.0",
    "Content-Length": "7964",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
    "X-Forwarded-For": "5.78.73.189",
    "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
    "X-Forwarded-Proto": "https",
    "X-Webhook-Signature": "e403034d3416e132e15b4f91e43e0578a20becee061f6f81b6385acebbb7a36b"
  },
  "raw": {
    "type": "matchResults",
    "payload": "W3siaWQiOiIyMDIzMTEtMS0xLTY2LTQwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEsImVhc3RJZCI6NjYsImVhc3RTaGlrb25hIjoiS2l0YW5vd2FrYSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNyBFYXN0Iiwid2VzdElkIjo0MCwid2VzdFNoaWtvbmEiOiJOaXNoaWtpZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNiBXZXN0Iiwia2ltYXJpdGUiOiJ5b3JpdGFvc2hpIiwid2lubmVySWQiOjY2LCJ3aW5uZXJFbiI6IktpdGFub3dha2EiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0yLTU1LTcxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjIsImVhc3RJZCI6NTUsImVhc3RTaGlrb25hIjoiUm9nYSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNiBFYXN0Iiwid2VzdElkIjo3MSwid2VzdFNoaWtvbmEiOiJDaHVyYW5vdW1pIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDE1IFdlc3QiLCJraW1hcml0ZSI6InlvcmlraXJpIiwid2lubmVySWQiOjcxLCJ3aW5uZXJFbiI6IkNodXJhbm91bWkiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0zLTU0LTExIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjMsImVhc3RJZCI6NTQsImVhc3RTaGlrb25hIjoiVG9oYWt1cnl1IiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDE1IEVhc3QiLCJ3ZXN0SWQiOjExLCJ3ZXN0U2hpa29uYSI6IkljaGl5YW1hbW90byIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNCBXZXN0Iiwia2ltYXJpdGUiOiJvc2hpZGFzaGkiLCJ3aW5uZXJJZCI6MTEsIndpbm5lckVuIjoiSWNoaXlhbWFtb3RvIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtNC0xMDItMzEiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6NCwiZWFzdElkIjoxMDIsImVhc3RTaGlrb25hIjoiVG9tb2themUiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTQgRWFzdCIsIndlc3RJZCI6MzEsIndlc3RTaGlrb25hIjoiVHN1cnVnaXNobyIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxMyBXZXN0Iiwia2ltYXJpdGUiOiJ1d2F0ZW5hZ2UiLCJ3aW5uZXJJZCI6MzEsIndpbm5lckVuIjoiVHN1cnVnaXNobyIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTUtMjUtMTQiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6NSwiZWFzdElkIjoyNSwiZWFzdFNoaWtvbmEiOiJUYWthcmFmdWppIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDEzIEVhc3QiLCJ3ZXN0SWQiOjE0LCJ3ZXN0U2hpa29uYSI6IlRhbWF3YXNoaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxMiBXZXN0Iiwia2ltYXJpdGUiOiJvc2hpZGFzaGkiLCJ3aW5uZXJJZCI6MTQsIndpbm5lckVuIjoiVGFtYXdhc2hpIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtNi00MS0yNCIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjo2LCJlYXN0SWQiOjQxLCJlYXN0U2hpa29uYSI6Ik9obyIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxMiBFYXN0Iiwid2VzdElkIjoyNCwid2VzdFNoaWtvbmEiOiJIaXJhZG91bWkiLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMTEgV2VzdCIsImtpbWFyaXRlIjoidHN1a2lkYXNoaSIsIndpbm5lcklkIjo0MSwid2lubmVyRW4iOiJPaG8iLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS03LTM1LTMwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjcsImVhc3RJZCI6MzUsImVhc3RTaGlrb25hIjoiU2FkYW5vdW1pIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDExIEVhc3QiLCJ3ZXN0SWQiOjMwLCJ3ZXN0U2hpa29uYSI6IktvdG9la28iLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMTAgV2VzdCIsImtpbWFyaXRlIjoieW9yaXRhb3NoaSIsIndpbm5lcklkIjozNSwid2lubmVyRW4iOiJTYWRhbm91bWkiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS04LTE1LTI2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjgsImVhc3RJZCI6MTUsImVhc3RTaGlrb25hIjoiUnl1ZGVuIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDEwIEVhc3QiLCJ3ZXN0SWQiOjI2LCJ3ZXN0U2hpa29uYSI6Ik1pdGFrZXVtaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA5IFdlc3QiLCJraW1hcml0ZSI6InlvcmlraXJpIiwid2lubmVySWQiOjE1LCJ3aW5uZXJFbiI6IlJ5dWRlbiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTktMzYtNzQiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6OSwiZWFzdElkIjozNiwiZWFzdFNoaWtvbmEiOiJNeW9naXJ5dSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA5IEVhc3QiLCJ3ZXN0SWQiOjc0LCJ3ZXN0U2hpa29uYSI6IkF0YW1pZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA4IFdlc3QiLCJraW1hcml0ZSI6ImtvdGVuYWdlIiwid2lubmVySWQiOjc0LCJ3aW5uZXJFbiI6IkF0YW1pZnVqaSIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEwLTE3LTUwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEwLCJlYXN0SWQiOjE3LCJlYXN0U2hpa29uYSI6IkVuZG8iLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgOCBFYXN0Iiwid2VzdElkIjo1MCwid2VzdFNoaWtvbmEiOiJLaW5ib3phbiIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA3IFdlc3QiLCJraW1hcml0ZSI6Im9zaGlkYXNoaSIsIndpbm5lcklkIjo1MCwid2lubmVyRW4iOiJLaW5ib3phbiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTExLTUzLTM3IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjExLCJlYXN0SWQiOjUzLCJlYXN0U2hpa29uYSI6Ikhva3VzZWlobyIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA3IEVhc3QiLCJ3ZXN0SWQiOjM3LCJ3ZXN0U2hpa29uYSI6IlRha2Fub3NobyIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA2IFdlc3QiLCJraW1hcml0ZSI6InlvcmlraXJpIiwid2lubmVySWQiOjUzLCJ3aW5uZXJFbiI6Ikhva3VzZWlobyIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEyLTQ5LTM0IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEyLCJlYXN0SWQiOjQ5LCJlYXN0U2hpa29uYSI6IlNob25hbm5vdW1pIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDYgRWFzdCIsIndlc3RJZCI6MzQsIndlc3RTaGlrb25hIjoiTWlkb3JpZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA1IFdlc3QiLCJraW1hcml0ZSI6ImhhdGFraWtvbWkiLCJ3aW5uZXJJZCI6NDksIndpbm5lckVuIjoiU2hvbmFubm91bWkiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0xMy0xMC0xNiIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxMywiZWFzdElkIjoxMCwiZWFzdFNoaWtvbmEiOiJPbm9zaG8iLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgNSBFYXN0Iiwid2VzdElkIjoxNiwid2VzdFNoaWtvbmEiOiJOaXNoaWtpZ2kiLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgNCBXZXN0Iiwia2ltYXJpdGUiOiJvc2hpZGFzaGkiLCJ3aW5uZXJJZCI6MTAsIndpbm5lckVuIjoiT25vc2hvIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMTQtMjItNTYiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6MTQsImVhc3RJZCI6MjIsImVhc3RTaGlrb25hIjoiQWJpIiwiZWFzdFJhbmsiOiJLb211c3ViaSAxIEVhc3QiLCJ3ZXN0SWQiOjU2LCJ3ZXN0U2hpa29uYSI6Ikdvbm95YW1hIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDQgRWFzdCIsImtpbWFyaXRlIjoib3NoaWRhc2hpIiwid2lubmVySWQiOjIyLCJ3aW5uZXJFbiI6IkFiaSIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE1LTIwLTIxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE1LCJlYXN0SWQiOjIwLCJlYXN0U2hpa29uYSI6IktvdG9ub3dha2EiLCJlYXN0UmFuayI6IlNla2l3YWtlIDIgRWFzdCIsIndlc3RJZCI6MjEsIndlc3RTaGlrb25hIjoiVG9iaXphcnUiLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMyBXZXN0Iiwia2ltYXJpdGUiOiJvc2hpZGFzaGkiLCJ3aW5uZXJJZCI6MjAsIndpbm5lckVuIjoiS290b25vd2FrYSIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE2LTQ0LTEzIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE2LCJlYXN0SWQiOjQ0LCJlYXN0U2hpa29uYSI6IlRha2F5YXN1IiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDMgRWFzdCIsIndlc3RJZCI6MTMsIndlc3RTaGlrb25hIjoiV2FrYW1vdG9oYXJ1Iiwid2VzdFJhbmsiOiJTZWtpd2FrZSAxIFdlc3QiLCJraW1hcml0ZSI6InV3YXRlbmFnZSIsIndpbm5lcklkIjo0NCwid2lubmVyRW4iOiJUYWtheWFzdSIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE3LTktMzgiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6MTcsImVhc3RJZCI6OSwiZWFzdFNoaWtvbmEiOiJEYWllaXNobyIsImVhc3RSYW5rIjoiU2VraXdha2UgMSBFYXN0Iiwid2VzdElkIjozOCwid2VzdFNoaWtvbmEiOiJNZWlzZWkiLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMiBXZXN0Iiwia2ltYXJpdGUiOiJ0c3VraWRhc2hpIiwid2lubmVySWQiOjksIndpbm5lckVuIjoiRGFpZWlzaG8iLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0xOC0zMy0xOSIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxOCwiZWFzdElkIjozMywiZWFzdFNoaWtvbmEiOiJTaG9kYWkiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMiBFYXN0Iiwid2VzdElkIjoxOSwid2VzdFNoaWtvbmEiOiJIb3Nob3J5dSIsIndlc3RSYW5rIjoiT3pla2kgMiBXZXN0Iiwia2ltYXJpdGUiOiJ0c3VraWRhc2hpIiwid2lubmVySWQiOjE5LCJ3aW5uZXJFbiI6Ikhvc2hvcnl1Iiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMTktMjgtNyIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxOSwiZWFzdElkIjoyOCwiZWFzdFNoaWtvbmEiOiJVcmEiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMSBXZXN0Iiwid2VzdElkIjo3LCJ3ZXN0U2hpa29uYSI6IktpcmlzaGltYSIsIndlc3RSYW5rIjoiT3pla2kgMSBXZXN0Iiwia2ltYXJpdGUiOiJ5b3JpdGFvc2hpIiwid2lubmVySWQiOjcsIndpbm5lckVuIjoiS2lyaXNoaW1hIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMjAtMS0yNyIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoyMCwiZWFzdElkIjoxLCJlYXN0U2hpa29uYSI6IlRha2FrZWlzaG8iLCJlYXN0UmFuayI6Ik96ZWtpIDEgRWFzdCIsIndlc3RJZCI6MjcsIndlc3RTaGlrb25hIjoiSG9rdXRvZnVqaSIsIndlc3RSYW5rIjoiS29tdXN1YmkgMSBXZXN0Iiwia2ltYXJpdGUiOiJvc2hpZGFzaGkiLCJ3aW5uZXJJZCI6MSwid2lubmVyRW4iOiJUYWtha2Vpc2hvIiwid2lubmVySnAiOiIifV0="
  },
  "payload_decoded": [
    {
      "id": "202311-1-1-66-40",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 1,
      "eastId": 66,
      "eastShikona": "Kitanowaka",
      "eastRank": "Maegashira 17 East",
      "westId": 40,
      "westShikona": "Nishikifuji",
      "westRank": "Maegashira 16 West",
      "kimarite": "yoritaoshi",
      "winnerId": 66,
      "winnerEn": "Kitanowaka",
      "winnerJp": ""
    },
    {
      "id": "202311-1-2-55-71",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 2,
      "eastId": 55,
      "eastShikona": "Roga",
      "eastRank": "Maegashira 16 East",
      "westId": 71,
      "westShikona": "Churanoumi",
      "westRank": "Maegashira 15 West",
      "kimarite": "yorikiri",
      "winnerId": 71,
      "winnerEn": "Churanoumi",
      "winnerJp": ""
    },
    {
      "id": "202311-1-3-54-11",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 3,
      "eastId": 54,
      "eastShikona": "Tohakuryu",
      "eastRank": "Maegashira 15 East",
      "westId": 11,
      "westShikona": "Ichiyamamoto",
      "westRank": "Maegashira 14 West",
      "kimarite": "oshidashi",
      "winnerId": 11,
      "winnerEn": "Ichiyamamoto",
      "winnerJp": ""
    },
    {
      "id": "202311-1-4-102-31",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 4,
      "eastId": 102,
      "eastShikona": "Tomokaze",
      "eastRank": "Maegashira 14 East",
      "westId": 31,
      "westShikona": "Tsurugisho",
      "westRank": "Maegashira 13 West",
      "kimarite": "uwatenage",
      "winnerId": 31,
      "winnerEn": "Tsurugisho",
      "winnerJp": ""
    },
    {
      "id": "202311-1-5-25-14",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 5,
      "eastId": 25,
      "eastShikona": "Takarafuji",
      "eastRank": "Maegashira 13 East",
      "westId": 14,
      "westShikona": "Tamawashi",
      "westRank": "Maegashira 12 West",
      "kimarite": "oshidashi",
      "winnerId": 14,
      "winnerEn": "Tamawashi",
      "winnerJp": ""
    },
    {
      "id": "202311-1-6-41-24",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 6,
      "eastId": 41,
      "eastShikona": "Oho",
      "eastRank": "Maegashira 12 East",
      "westId": 24,
      "westShikona": "Hiradoumi",
      "westRank": "Maegashira 11 West",
      "kimarite": "tsukidashi",
      "winnerId": 41,
      "winnerEn": "Oho",
      "winnerJp": ""
    },
    {
      "id": "202311-1-7-35-30",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 7,
      "eastId": 35,
      "eastShikona": "Sadanoumi",
      "eastRank": "Maegashira 11 East",
      "westId": 30,
      "westShikona": "Kotoeko",
      "westRank": "Maegashira 10 West",
      "kimarite": "yoritaoshi",
      "winnerId": 35,
      "winnerEn": "Sadanoumi",
      "winnerJp": ""
    },
    {
      "id": "202311-1-8-15-26",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 8,
      "eastId": 15,
      "eastShikona": "Ryuden",
      "eastRank": "Maegashira 10 East",
      "westId": 26,
      "westShikona": "Mitakeumi",
      "westRank": "Maegashira 9 West",
      "kimarite": "yorikiri",
      "winnerId": 15,
      "winnerEn": "Ryuden",
      "winnerJp": ""
    },
    {
      "id": "202311-1-9-36-74",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 9,
      "eastId": 36,
      "eastShikona": "Myogiryu",
      "eastRank": "Maegashira 9 East",
      "westId": 74,
      "westShikona": "Atamifuji",
      "westRank": "Maegashira 8 West",
      "kimarite": "kotenage",
      "winnerId": 74,
      "winnerEn": "Atamifuji",
      "winnerJp": ""
    },
    {
      "id": "202311-1-10-17-50",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 10,
      "eastId": 17,
      "eastShikona": "Endo",
      "eastRank": "Maegashira 8 East",
      "westId": 50,
      "westShikona": "Kinbozan",
      "westRank": "Maegashira 7 West",
      "kimarite": "oshidashi",
      "winnerId": 50,
      "winnerEn": "Kinbozan",
      "winnerJp": ""
    },
    {
      "id": "202311-1-11-53-37",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 11,
      "eastId": 53,
      "eastShikona": "Hokuseiho",
      "eastRank": "Maegashira 7 East",
      "westId": 37,
      "westShikona": "Takanosho",
      "westRank": "Maegashira 6 West",
      "kimarite": "yorikiri",
      "winnerId": 53,
      "winnerEn": "Hokuseiho",
      "winnerJp": ""
    },
    {
      "id": "202311-1-12-49-34",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 12,
      "eastId": 49,
      "eastShikona": "Shonannoumi",
      "eastRank": "Maegashira 6 East",
      "westId": 34,
      "westShikona": "Midorifuji",
      "westRank": "Maegashira 5 West",
      "kimarite": "hatakikomi",
      "winnerId": 49,
      "winnerEn": "Shonannoumi",
      "winnerJp": ""
    },
    {
      "id": "202311-1-13-10-16",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 13,
      "eastId": 10,
      "eastShikona": "Onosho",
      "eastRank": "Maegashira 5 East",
      "westId": 16,
      "westShikona": "Nishikigi",
      "westRank": "Maegashira 4 West",
      "kimarite": "oshidashi",
      "winnerId": 10,
      "winnerEn": "Onosho",
      "winnerJp": ""
    },
    {
      "id": "202311-1-14-22-56",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 14,
      "eastId": 22,
      "eastShikona": "Abi",
      "eastRank": "Komusubi 1 East",
      "westId": 56,
      "westShikona": "Gonoyama",
      "westRank": "Maegashira 4 East",
      "kimarite": "oshidashi",
      "winnerId": 22,
      "winnerEn": "Abi",
      "winnerJp": ""
    },
    {
      "id": "202311-1-15-20-21",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 15,
      "eastId": 20,
      "eastShikona": "Kotonowaka",
      "eastRank": "Sekiwake 2 East",
      "westId": 21,
      "westShikona": "Tobizaru",
      "westRank": "Maegashira 3 West",
      "kimarite": "oshidashi",
      "winnerId": 20,
      "winnerEn": "Kotonowaka",
      "winnerJp": ""
    },
    {
      "id": "202311-1-16-44-13",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 16,
      "eastId": 44,
      "eastShikona": "Takayasu",
      "eastRank": "Maegashira 3 East",
      "westId": 13,
      "westShikona": "Wakamotoharu",
      "westRank": "Sekiwake 1 West",
      "kimarite": "uwatenage",
      "winnerId": 44,
      "winnerEn": "Takayasu",
      "winnerJp": ""
    },
    {
      "id": "202311-1-17-9-38",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 17,
      "eastId": 9,
      "eastShikona": "Daieisho",
      "eastRank": "Sekiwake 1 East",
      "westId": 38,
      "westShikona": "Meisei",
      "westRank": "Maegashira 2 West",
      "kimarite": "tsukidashi",
      "winnerId": 9,
      "winnerEn": "Daieisho",
      "winnerJp": ""
    },
    {
      "id": "202311-1-18-33-19",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 18,
      "eastId": 33,
      "eastShikona": "Shodai",
      "eastRank": "Maegashira 2 East",
      "westId": 19,
      "westShikona": "Hoshoryu",
      "westRank": "Ozeki 2 West",
      "kimarite": "tsukidashi",
      "winnerId": 19,
      "winnerEn": "Hoshoryu",
      "winnerJp": ""
    },
    {
      "id": "202311-1-19-28-7",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 19,
      "eastId": 28,
      "eastShikona": "Ura",
      "eastRank": "Maegashira 1 West",
      "westId": 7,
      "westShikona": "Kirishima",
      "westRank": "Ozeki 1 West",
      "kimarite": "yoritaoshi",
      "winnerId": 7,
      "winnerEn": "Kirishima",
      "winnerJp": ""
    },
    {
      "id": "202311-1-20-1-27",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 20,
      "eastId": 1,
      "eastShikona": "Takakeisho",
      "eastRank": "Ozeki 1 East",
      "westId": 27,
      "westShikona": "Hokutofuji",
      "westRank": "Komusubi 1 West",
      "kimarite": "oshidashi",
      "winnerId": 1,
      "winnerEn": "Takakeisho",
      "winnerJp": ""
    }
  ]
}
  
  
  
  start_task = start_msg()
  
  spark_conf = spark_conf()


  # Push webhook JSON into XCom early so any downstream branch tasks can access it.
  push_webhook_xcom = PythonOperator(
    task_id="push_webhook_xcom",
    python_callable=call_push_webhook_xcom,
    op_kwargs={"webhook": webhook},
  )

  postgres_branch_task = choose_job(webhook, "postgres")

  mongo_branch_task = choose_job(webhook, "mongo")

  new_basho_postgres = PythonOperator(
      task_id="run_new_basho_postgres",
      python_callable=call_new_basho_postgres,
      op_kwargs={"webhook": webhook},
  )

  end_basho_postgres = PythonOperator(
      task_id="run_end_basho_postgres",
      python_callable=call_end_basho_postgres,
      op_kwargs={"webhook": webhook},
  )

  new_matches_postgres = PythonOperator(
      task_id="run_new_matches_postgres",
      python_callable=call_new_matches_postgres,
      op_kwargs={"webhook": webhook},
  )

  match_results_postgres = PythonOperator(
      task_id="run_match_results_postgres",
      python_callable=call_match_results_postgres,
      op_kwargs={"webhook": webhook},
  )
  
  skip_postgres = PythonOperator(
    task_id="skip_jobs_postgres",
    python_callable=call_skip,
    op_kwargs={"webhook": webhook},
  )

  skip_mongo = PythonOperator(
    task_id="skip_jobs_mongo",
    python_callable=call_skip,
    op_kwargs={"webhook": webhook},
  )
  

  homepage_task = SparkSubmitOperator(
    task_id="run_homepage",
    application="/opt/airflow/jobs/spark_homepage.py",
    conn_id="spark_default",
    packages="org.postgresql:postgresql:42.6.0",
    driver_memory="4g",
    executor_memory="4g",
    executor_cores=2,
    name="spark_homepage_job",
    conf={
        "spark.pyspark.python": "python3",
        "spark.executorEnv.PYSPARK_PYTHON": "python3",
        "spark.pyspark.driver.python": "python3",
        "spark.jars": "/opt/spark/jars/postgresql-42.6.0.jar",
        "spark.executorEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_URI'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_COLL_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_URI'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_COLL_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.DB_HOST": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.DB_HOST'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.DB_PORT": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.DB_PORT'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.DB_USERNAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_USERNAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.DB_PASSWORD": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_PASSWORD'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.DB_HOST": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_HOST'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.DB_PORT": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_PORT'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.DB_USERNAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_USERNAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.DB_PASSWORD": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.DB_PASSWORD'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
    },
      application_args=[
    "--jdbc-url", "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['jdbc_url'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
      ],
      do_xcom_push=False,
  )

  run_new_matches_mongo = SparkSubmitOperator(
    task_id="run_new_matches_mongo",
    application="/opt/airflow/jobs/spark_mongoNewMatches.py",
    conn_id="spark_default",
    packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
    application_args=["{{ ti.xcom_pull(task_ids='push_webhook_xcom') }}"],
    driver_memory="2g",
    executor_memory="2g",
    conf={
        "spark.pyspark.python": "python3",
        "spark.executorEnv.PYSPARK_PYTHON": "python3",
        "spark.pyspark.driver.python": "python3",
        "spark.executorEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_URI'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_COLL_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_URI'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_COLL_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
    },
  )
  
  run_new_basho_mongo = PythonOperator(
    task_id="run_new_basho_mongo",
    python_callable=call_new_basho_mongo,
    op_kwargs={"webhook": webhook},
  )
  
  run_end_basho_mongo = PythonOperator(
    task_id="run_end_basho_mongo",
    python_callable=call_end_basho_mongo,
    op_kwargs={"webhook": webhook},
  )

  run_match_results_mongo = SparkSubmitOperator(
    task_id="run_match_results_mongo",
    application="/opt/airflow/jobs/spark_mongoMatchResults.py",
    conn_id="spark_default",
    packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
    application_args=["{{ ti.xcom_pull(task_ids='push_webhook_xcom') }}"],
    driver_memory="2g",
    executor_memory="2g",
    executor_cores=2,
    name="spark_mongo_new_matches_job",
    conf={
        "spark.pyspark.python": "python3",
        "spark.executorEnv.PYSPARK_PYTHON": "python3",
        "spark.pyspark.driver.python": "python3",
        "spark.executorEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_URI'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.executorEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.executorEnv.MONGO_COLL_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_URI": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_URI'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_DB_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_DB_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
        "spark.driverEnv.MONGO_COLL_NAME": "{{ ti.xcom_pull(task_ids='spark_conf', key='return_value')['conf']['spark.driverEnv.MONGO_COLL_NAME'] if ti.xcom_pull(task_ids='spark_conf', key='return_value') else '' }}",
    },
  )

  join_postgres = EmptyOperator(task_id="join_postgres", trigger_rule="one_success")

  join_mongo = EmptyOperator(task_id="join_mongo", trigger_rule="one_success")
  
  run_spark_data_cleaning = SparkSubmitOperator(
    task_id="run_data_cleaning",
    application="/opt/airflow/jobs/spark_data_cleaning.py",
    conn_id="spark_default",
    driver_memory="4g",
    executor_memory="4g",
    executor_cores=3,
    name="spark_data_cleaning_job",
    conf={
        # python plumbing
        "spark.pyspark.python": "python3",
        "spark.executorEnv.PYSPARK_PYTHON": "python3",
        "spark.pyspark.driver.python": "python3",

        # S3A base (matches your script)
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",

        # MINIMAL AWS from Airflow container env (NO JINJA)
        "spark.executorEnv.AWS_REGION": os.environ.get("AWS_REGION", "us-west-2"),
        "spark.executorEnv.AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        
        "spark.hadoop.fs.s3a.connections.maximum": "200",
        "spark.hadoop.fs.s3a.threads.max": "200",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
        "spark.hadoop.fs.s3a.connection.timeout": "10000",
    },
    application_args=[
      "--input", "{{ dag_run.conf.get('input','s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/') }}",
      "--output", "{{ dag_run.conf.get('output','s3a://ryans-sumo-bucket/silver/rikishi_matches/') }}",
    ],
    do_xcom_push=False,
  )
  
  run_spark_ml_dataset = SparkSubmitOperator(
    task_id="run_ml_dataset",
    application="/opt/airflow/jobs/spark_ml_dataset.py",
    conn_id="spark_default",
    driver_memory="4g",
    executor_memory="4g",
    executor_cores=3,
    name="spark_ml_dataset_job",
    conf={
        # python plumbing
        "spark.pyspark.python": "python3",
        "spark.executorEnv.PYSPARK_PYTHON": "python3",
        "spark.pyspark.driver.python": "python3",

        # S3A base (matches your script)
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",

        # MINIMAL AWS from Airflow container env (NO JINJA)
        "spark.executorEnv.AWS_REGION": os.environ.get("AWS_REGION", "us-west-2"),
        "spark.executorEnv.AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        
        "spark.hadoop.fs.s3a.connections.maximum": "200",
        "spark.hadoop.fs.s3a.threads.max": "200",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
        "spark.hadoop.fs.s3a.connection.timeout": "10000",
    },
    application_args=[
      "--input", "{{ dag_run.conf.get('input','s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/') }}",
      "--output", "{{ dag_run.conf.get('output','s3a://ryans-sumo-bucket/silver/rikishi_matches/') }}",
    ],
    do_xcom_push=False,
  )
  
  
  run_spark_ml_training = SparkSubmitOperator(
    task_id="run_ml_training",
    application="/opt/airflow/jobs/spark_ml_training.py",
    conn_id="spark_default",
    driver_memory="4g",
    executor_memory="4g",
    executor_cores=3,
    name="spark_ml_training_job",
    conf={
        # python plumbing
        "spark.pyspark.python": "python3",
        "spark.executorEnv.PYSPARK_PYTHON": "python3",
        "spark.pyspark.driver.python": "python3",

        # S3A base (matches your script)
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",

        # MINIMAL AWS from Airflow container env (NO JINJA)
        "spark.executorEnv.AWS_REGION": os.environ.get("AWS_REGION", "us-west-2"),
        "spark.executorEnv.AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        
        "spark.hadoop.fs.s3a.connections.maximum": "200",
        "spark.hadoop.fs.s3a.threads.max": "200",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
        "spark.hadoop.fs.s3a.connection.timeout": "10000",
    },
    application_args=[
      "--input", "{{ dag_run.conf.get('input','s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/') }}",
      "--output", "{{ dag_run.conf.get('output','s3a://ryans-sumo-bucket/silver/rikishi_matches/') }}",
    ],
    do_xcom_push=False,
  )
  

  end_task = end_msg()



  spark_conf >> homepage_task
  spark_conf >> run_new_matches_mongo
  spark_conf >> run_match_results_mongo
  # Connect tasks/operators
  # run the spark smoke test first, then continue to the postgres branch
  start_task >> spark_conf
  spark_conf >> spark_smoke
  spark_smoke >> postgres_branch_task
  postgres_branch_task >> [new_basho_postgres, end_basho_postgres, new_matches_postgres, match_results_postgres, skip_postgres]
  [new_basho_postgres, end_basho_postgres, new_matches_postgres, match_results_postgres, skip_postgres] >> join_postgres
  # Run homepage and the mongo branch in parallel after postgres join
  join_postgres >> homepage_task

  join_postgres >> push_webhook_xcom

  # Branch downstream choices
  push_webhook_xcom >> mongo_branch_task >> [run_new_basho_mongo, run_new_matches_mongo, run_end_basho_mongo, run_match_results_mongo, skip_mongo] >> join_mongo

  # Both homepage and the mongo branch must finish before finishing the DAG
  homepage_task >> join_mongo
  join_mongo >> run_spark_data_cleaning >> run_spark_ml_dataset >> run_spark_ml_training >> end_task
