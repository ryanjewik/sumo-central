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
  
@task.branch
def ml_train_branch(webhook: dict):
    webhook_type = (webhook.get("type") or "").strip()
    mapping = {
    # Branch must return the task_id of an immediate downstream task.
    # The ML branch starts with `run_data_cleaning` (task_id="run_data_cleaning").
    # Returning `run_ml_dataset` (a downstream-of-downstream) caused the branch
    # to skip its immediate children and therefore skip the whole branch.
    "endBasho": "run_data_cleaning",
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
  dag_id="sumo_data_pipeline",
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
        executor_memory="1g",
        executor_cores=1,
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

  webhook = {
  "received_at": 1756357625,
  "type": "endBasho",
  "headers": {
    "Host": "74de6cbafcff.ngrok-free.app",
    "User-Agent": "Go-http-client/2.0",
    "Content-Length": "1508",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
    "X-Forwarded-For": "5.78.73.189",
    "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
    "X-Forwarded-Proto": "https",
    "X-Webhook-Signature": "da0c5ab394ec7ccc1966b464a010091b05a10320c845cc80b48760c023f12f7c"
  },
  "raw": {
    "type": "endBasho",
    "payload": "eyJkYXRlIjoiMjAyMzExIiwibG9jYXRpb24iOiJGdWt1b2thLCBGdWt1b2thIEludGVybmF0aW9uYWwgQ2VudGVyIiwic3RhcnREYXRlIjoiMjAyMy0xMS0xMlQwMDowMDowMFoiLCJlbmREYXRlIjoiMjAyMy0xMS0yNlQwMDowMDowMFoiLCJ5dXNobyI6W3sidHlwZSI6Ik1ha3V1Y2hpIiwicmlraXNoaUlkIjo3LCJzaGlrb25hRW4iOiJLaXJpc2hpbWEgVGV0c3VvIiwic2hpa29uYUpwIjoi6Zyn5bO244CA6ZC15YqbIn0seyJ0eXBlIjoiSnVyeW8iLCJyaWtpc2hpSWQiOjgsInNoaWtvbmFFbiI6IktvdG9zaG9obyBZb3NoaW5hcmkiLCJzaGlrb25hSnAiOiLnkLTli53ls7DjgIDlkInmiJAifSx7InR5cGUiOiJNYWt1c2hpdGEiLCJyaWtpc2hpSWQiOjYwOSwic2hpa29uYUVuIjoiU2F0b3J1ZnVqaSBUZXBwZWkiLCJzaGlrb25hSnAiOiLogZblr4zlo6so44GV44Go44KL44G144GYKSJ9LHsidHlwZSI6IlNhbmRhbm1lIiwicmlraXNoaUlkIjoyMzYsInNoaWtvbmFFbiI6IkRhaXNob3J5dSBIYXJ1Y2hpa2EiLCJzaGlrb25hSnAiOiLlpKfmmIfpvo0o44Gg44GE44GX44KH44GG44KK44KF44GGKSJ9LHsidHlwZSI6IkpvbmlkYW4iLCJyaWtpc2hpSWQiOjQ5OCwic2hpa29uYUVuIjoiRGFpcmluemFuIFJpbiIsInNoaWtvbmFKcCI6IuWkp+WHnOWxsSjjgaDjgYTjgorjgpPjgZbjgpMpIn0seyJ0eXBlIjoiSm9ub2t1Y2hpIiwicmlraXNoaUlkIjo4ODU0LCJzaGlrb25hRW4iOiJBb25pc2hpa2kgQXJhdGEiLCJzaGlrb25hSnAiOiLlronpnZLpjKbjgIDmlrDlpKcifV0sInNwZWNpYWxQcml6ZXMiOlt7InR5cGUiOiJLYW50by1zaG8iLCJyaWtpc2hpSWQiOjExLCJzaGlrb25hRW4iOiJJY2hpeWFtYW1vdG8gRGFpa2kiLCJzaGlrb25hSnAiOiLkuIDlsbHmnKzjgIDlpKfnlJ8ifSx7InR5cGUiOiJLYW50by1zaG8iLCJyaWtpc2hpSWQiOjIwLCJzaGlrb25hRW4iOiJLb3RvemFrdXJhIE1hc2FrYXRzdSIsInNoaWtvbmFKcCI6IueQtOaru+OAgOWwhuWCkSJ9LHsidHlwZSI6IkthbnRvLXNobyIsInJpa2lzaGlJZCI6NzQsInNoaWtvbmFFbiI6IkF0YW1pZnVqaSBTYWt1dGFybyIsInNoaWtvbmFKcCI6IueGsea1t+WvjOWjq+OAgOaclOWkqumDjiJ9XX0="
  },
  "payload_decoded": {
    "date": "202311",
    "location": "Fukuoka, Fukuoka International Center",
    "startDate": "2023-11-12T00:00:00Z",
    "endDate": "2023-11-26T00:00:00Z",
    "yusho": [
      {
        "type": "Makuuchi",
        "rikishiId": 7,
        "shikonaEn": "Kirishima Tetsuo",
        "shikonaJp": "霧島　鐵力"
      },
      {
        "type": "Juryo",
        "rikishiId": 8,
        "shikonaEn": "Kotoshoho Yoshinari",
        "shikonaJp": "琴勝峰　吉成"
      },
      {
        "type": "Makushita",
        "rikishiId": 609,
        "shikonaEn": "Satorufuji Teppei",
        "shikonaJp": "聖富士(さとるふじ)"
      },
      {
        "type": "Sandanme",
        "rikishiId": 236,
        "shikonaEn": "Daishoryu Haruchika",
        "shikonaJp": "大昇龍(だいしょうりゅう)"
      },
      {
        "type": "Jonidan",
        "rikishiId": 498,
        "shikonaEn": "Dairinzan Rin",
        "shikonaJp": "大凜山(だいりんざん)"
      },
      {
        "type": "Jonokuchi",
        "rikishiId": 8854,
        "shikonaEn": "Aonishiki Arata",
        "shikonaJp": "安青錦　新大"
      }
    ],
    "specialPrizes": [
      {
        "type": "Kanto-sho",
        "rikishiId": 11,
        "shikonaEn": "Ichiyamamoto Daiki",
        "shikonaJp": "一山本　大生"
      },
      {
        "type": "Kanto-sho",
        "rikishiId": 20,
        "shikonaEn": "Kotozakura Masakatsu",
        "shikonaJp": "琴櫻　将傑"
      },
      {
        "type": "Kanto-sho",
        "rikishiId": 74,
        "shikonaEn": "Atamifuji Sakutaro",
        "shikonaJp": "熱海富士　朔太郎"
      }
    ]
  }
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
  
  ml_train_branch_task = ml_train_branch(webhook)

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
  
  skip_train = PythonOperator(
    task_id="skip_ml_train",
    python_callable=call_skip,
    op_kwargs={"webhook": webhook},
  )
  

  homepage_task = SparkSubmitOperator(
    task_id="run_homepage",
    application="/opt/airflow/jobs/spark_homepage.py",
    conn_id="spark_default",
    packages="org.postgresql:postgresql:42.6.0",
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
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
        "spark.executor.instances": "1",
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
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
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
        "spark.executor.instances": "1",
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
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
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
        "spark.executor.instances": "1",
    },
  )

  join_postgres = EmptyOperator(task_id="join_postgres", trigger_rule="one_success")

  join_mongo = EmptyOperator(task_id="join_mongo", trigger_rule="one_success")
  
  run_spark_data_cleaning = SparkSubmitOperator(
    task_id="run_data_cleaning",
    application="/opt/airflow/jobs/spark_data_cleaning.py",
    conn_id="spark_default",
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
    jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
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
        "spark.executor.instances": os.environ.get("SPARK_WORKER_CORES", "2"),
        
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
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
    jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
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
        "spark.executor.instances": os.environ.get("SPARK_WORKER_CORES", "2"),
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
    driver_memory="1g",
    executor_memory="2g",
    executor_cores=1,
    jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
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
        "spark.executor.instances": "1",
        # expose the cluster slot hint into the Spark driver so the job can
        # observe the intended worker capacity (TOTAL_SLOTS). In docker-compose
        # SPARK_WORKER_CORES is set on the worker containers but not on the
        # spark driver; pass it explicitly here so the driver-side script can
        # read it via os.environ.get("TOTAL_SLOTS") or TOTAL_SLOTS.
        "spark.driverEnv.TOTAL_SLOTS": os.environ.get("SPARK_WORKER_CORES", "2"),
        
        "spark.network.timeout": "600s",
        "spark.executor.heartbeatInterval": "60s",
    },
    execution_timeout=timedelta(minutes=90),
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
  join_mongo >> ml_train_branch_task
  homepage_task >> ml_train_branch_task

  ml_train_branch_task >> run_spark_data_cleaning >> run_spark_ml_dataset >> run_spark_ml_training >> end_task
  ml_train_branch_task  >> skip_train >> end_task