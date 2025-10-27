from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import importlib.util
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import json
import os
import sys

@task
def start_msg():
    print("DAG starting")


@task
def end_msg():
    print("DAG ended")


@task.branch
def choose_job(webhook: dict, db_type: str):
    # For now, webhook will come from Kafka later. Use the provided webhook dict.
    webhook_type = (webhook.get("type") or "").strip()
    mapping = {
        "newBasho": f"run_new_basho_{db_type}",
        "endBasho": f"run_end_basho_{db_type}",
    # For mongo newMatches we want to run the XCom push first so the
    # Spark operator has the webhook payload available. Return the
    # push task id instead of the final run task when db_type is mongo.
    "newMatches": ("push_webhook_xcom" if db_type == "mongo" else f"run_new_matches_{db_type}"),
        "matchResults": f"run_match_results_{db_type}",
    }

    return mapping.get(webhook_type, "skip_jobs")


def _load_and_call(path: str, func_name: str, webhook: dict):
    """Dynamically load a module from `path` and call `func_name(webhook)` if present."""
    # Ensure the jobs directory is on sys.path so imports like `from utils...` work
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
  # ignore webhook payload here; homepage job reads DB directly
  # Use the Spark-first homepage implementation so Airflow can submit the
  # job to the Spark cluster. The module `spark_homepage.py` exposes
  # `run_homepage_job` which prefers Spark JDBC reads and supports
  # executor-side Mongo writes.
  return _load_and_call("/opt/airflow/jobs/spark_homepage.py", "run_homepage_job", webhook)


def call_new_matches_mongo(webhook: dict):
  return _load_and_call("/opt/airflow/jobs/mongoNewMatches.py", "process_new_matches", webhook)


def call_push_webhook_xcom(webhook: dict):
  """Return webhook JSON string to be pushed to XCom for SparkSubmitOperator."""
  return json.dumps(webhook)


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
    driver_memory="1g", executor_memory="2g", executor_cores=2,
    # verbose=True,
  )
    
  #2. mongo updates (can be done concurrently)
  #3. bronze to silver
  #4. silver to gold
  #5. update ML training set
  #6. trigger model retraining

  webhook = {
  "received_at": 1756357623,
  "type": "newMatches",
  "headers": {
    "Host": "74de6cbafcff.ngrok-free.app",
    "User-Agent": "Go-http-client/2.0",
    "Content-Length": "7470",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
    "X-Forwarded-For": "5.78.73.189",
    "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
    "X-Forwarded-Proto": "https",
    "X-Webhook-Signature": "82b3a91f97f1dad97463ac00907b9914adcd80aec013498e5b14c3b339f23c6e"
  },
  "raw": {
    "type": "newMatches",
    "payload": "W3siaWQiOiIyMDIzMTEtMS0xLTY2LTQwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEsImVhc3RJZCI6NjYsImVhc3RTaGlrb25hIjoiS2l0YW5vd2FrYSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNyBFYXN0Iiwid2VzdElkIjo0MCwid2VzdFNoaWtvbmEiOiJOaXNoaWtpZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNiBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0yLTU1LTcxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjIsImVhc3RJZCI6NTUsImVhc3RTaGlrb25hIjoiUm9nYSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNiBFYXN0Iiwid2VzdElkIjo3MSwid2VzdFNoaWtvbmEiOiJDaHVyYW5vdW1pIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDE1IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTMtNTQtMTEiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6MywiZWFzdElkIjo1NCwiZWFzdFNoaWtvbmEiOiJUb2hha3VyeXUiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTUgRWFzdCIsIndlc3RJZCI6MTEsIndlc3RTaGlrb25hIjoiSWNoaXlhbWFtb3RvIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDE0IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTQtMTAyLTMxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjQsImVhc3RJZCI6MTAyLCJlYXN0U2hpa29uYSI6IlRvbW9rYXplIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDE0IEVhc3QiLCJ3ZXN0SWQiOjMxLCJ3ZXN0U2hpa29uYSI6IlRzdXJ1Z2lzaG8iLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMTMgV2VzdCIsImtpbWFyaXRlIjoiIiwid2lubmVySWQiOjAsIndpbm5lckVuIjoiIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtNS0yNS0xNCIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjo1LCJlYXN0SWQiOjI1LCJlYXN0U2hpa29uYSI6IlRha2FyYWZ1amkiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTMgRWFzdCIsIndlc3RJZCI6MTQsIndlc3RTaGlrb25hIjoiVGFtYXdhc2hpIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDEyIFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTYtNDEtMjQiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6NiwiZWFzdElkIjo0MSwiZWFzdFNoaWtvbmEiOiJPaG8iLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTIgRWFzdCIsIndlc3RJZCI6MjQsIndlc3RTaGlrb25hIjoiSGlyYWRvdW1pIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDExIFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTctMzUtMzAiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6NywiZWFzdElkIjozNSwiZWFzdFNoaWtvbmEiOiJTYWRhbm91bWkiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTEgRWFzdCIsIndlc3RJZCI6MzAsIndlc3RTaGlrb25hIjoiS290b2VrbyIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxMCBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS04LTE1LTI2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjgsImVhc3RJZCI6MTUsImVhc3RTaGlrb25hIjoiUnl1ZGVuIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDEwIEVhc3QiLCJ3ZXN0SWQiOjI2LCJ3ZXN0U2hpa29uYSI6Ik1pdGFrZXVtaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA5IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTktMzYtNzQiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6OSwiZWFzdElkIjozNiwiZWFzdFNoaWtvbmEiOiJNeW9naXJ5dSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA5IEVhc3QiLCJ3ZXN0SWQiOjc0LCJ3ZXN0U2hpa29uYSI6IkF0YW1pZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA4IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEwLTE3LTUwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEwLCJlYXN0SWQiOjE3LCJlYXN0U2hpa29uYSI6IkVuZG8iLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgOCBFYXN0Iiwid2VzdElkIjo1MCwid2VzdFNoaWtvbmEiOiJLaW5ib3phbiIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA3IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTExLTUzLTM3IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjExLCJlYXN0SWQiOjUzLCJlYXN0U2hpa29uYSI6Ikhva3VzZWlobyIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA3IEVhc3QiLCJ3ZXN0SWQiOjM3LCJ3ZXN0U2hpa29uYSI6IlRha2Fub3NobyIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA2IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEyLTQ5LTM0IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEyLCJlYXN0SWQiOjQ5LCJlYXN0U2hpa29uYSI6IlNob25hbm5vdW1pIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDYgRWFzdCIsIndlc3RJZCI6MzQsIndlc3RTaGlrb25hIjoiTWlkb3JpZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA1IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEzLTEwLTE2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEzLCJlYXN0SWQiOjEwLCJlYXN0U2hpa29uYSI6Ik9ub3NobyIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA1IEVhc3QiLCJ3ZXN0SWQiOjE2LCJ3ZXN0U2hpa29uYSI6Ik5pc2hpa2lnaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA0IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE0LTIyLTU2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE0LCJlYXN0SWQiOjIyLCJlYXN0U2hpa29uYSI6IkFiaSIsImVhc3RSYW5rIjoiS29tdXN1YmkgMSBFYXN0Iiwid2VzdElkIjo1Niwid2VzdFNoaWtvbmEiOiJHb25veWFtYSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA0IEVhc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE1LTIwLTIxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE1LCJlYXN0SWQiOjIwLCJlYXN0U2hpa29uYSI6IktvdG9ub3dha2EiLCJlYXN0UmFuayI6IlNla2l3YWtlIDIgRWFzdCIsIndlc3RJZCI6MjEsIndlc3RTaGlrb25hIjoiVG9iaXphcnUiLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMyBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0xNi00NC0xMyIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxNiwiZWFzdElkIjo0NCwiZWFzdFNoaWtvbmEiOiJUYWtheWFzdSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAzIEVhc3QiLCJ3ZXN0SWQiOjEzLCJ3ZXN0U2hpa29uYSI6Ildha2Ftb3RvaGFydSIsIndlc3RSYW5rIjoiU2VraXdha2UgMSBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0xNy05LTM4IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE3LCJlYXN0SWQiOjksImVhc3RTaGlrb25hIjoiRGFpZWlzaG8iLCJlYXN0UmFuayI6IlNla2l3YWtlIDEgRWFzdCIsIndlc3RJZCI6MzgsIndlc3RTaGlrb25hIjoiTWVpc2VpIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDIgV2VzdCIsImtpbWFyaXRlIjoiIiwid2lubmVySWQiOjAsIndpbm5lckVuIjoiIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMTgtMzMtMTkiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6MTgsImVhc3RJZCI6MzMsImVhc3RTaGlrb25hIjoiU2hvZGFpIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDIgRWFzdCIsIndlc3RJZCI6MTksIndlc3RTaGlrb25hIjoiSG9zaG9yeXUiLCJ3ZXN0UmFuayI6Ik96ZWtpIDIgV2VzdCIsImtpbWFyaXRlIjoiIiwid2lubmVySWQiOjAsIndpbm5lckVuIjoiIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMTktMjgtNyIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxOSwiZWFzdElkIjoyOCwiZWFzdFNoaWtvbmEiOiJVcmEiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMSBXZXN0Iiwid2VzdElkIjo3LCJ3ZXN0U2hpa29uYSI6IktpcmlzaGltYSIsIndlc3RSYW5rIjoiT3pla2kgMSBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0yMC0xLTI3IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjIwLCJlYXN0SWQiOjEsImVhc3RTaGlrb25hIjoiVGFrYWtlaXNobyIsImVhc3RSYW5rIjoiT3pla2kgMSBFYXN0Iiwid2VzdElkIjoyNywid2VzdFNoaWtvbmEiOiJIb2t1dG9mdWppIiwid2VzdFJhbmsiOiJLb211c3ViaSAxIFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn1d"
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
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
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    }
  ]
  }

  start_task = start_msg()
  
  spark_smoke #spark test

  # Push webhook JSON into XCom early so any downstream branch tasks can access it.
  push_webhook_xcom = PythonOperator(
    task_id="push_webhook_xcom",
    python_callable=call_push_webhook_xcom,
    op_kwargs={"webhook": webhook},
    # leave XCom enabled so the Spark operator can pull the JSON string
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

  # Build JDBC URL from the Airflow connection `sumo_db` when possible so
  # the Spark driver receives a correct host (e.g. `sumo-db`) instead of
  # defaulting to localhost when env vars aren't forwarded into the driver.
  try:
    sumo_conn = BaseHook.get_connection("sumo_db")
    conn_host = sumo_conn.host or os.getenv("DB_HOST", "sumo-db")
    conn_port = sumo_conn.port or os.getenv("DB_PORT", "5432")
    conn_db = sumo_conn.schema or os.getenv("DB_NAME", "sumo")
    jdbc_url = f"jdbc:postgresql://{conn_host}:{conn_port}/{conn_db}"
  except Exception:
    # Fall back to environment/compose default
    jdbc_url = os.environ.get("JDBC_URL") or f"jdbc:postgresql://sumo-db:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME','sumo')}"

  # Run the Spark-first homepage job via SparkSubmit so the driver/executors
  # run inside the Spark cluster. This avoids importing Airflow hooks inside
  # the Spark runtime and lets executors perform the Mongo upsert.
  homepage_task = SparkSubmitOperator(
    task_id="run_homepage",
    application="/opt/airflow/jobs/spark_homepage.py",
    conn_id="spark_default",
    # Ask Spark to fetch the Postgres JDBC driver from Maven if the client
    # cannot access a local jar path. This is more reliable when spark-submit
    # runs from a different container than the Spark nodes.
    packages="org.postgresql:postgresql:42.6.0",
    driver_memory="1g",
    executor_memory="2g",
    executor_cores=2,
    name="spark_homepage_job",
    conf={
      "spark.pyspark.python": "python3",
      "spark.executorEnv.PYSPARK_PYTHON": "python3",
      "spark.pyspark.driver.python": "python3",
      # Ensure the Postgres JDBC jar provided in the Spark image is
      # explicitly added to spark-submit so the driver JVM can load
      # org.postgresql.Driver when reading via JDBC.
      "spark.jars": "/opt/spark/jars/postgresql-42.6.0.jar",
    },
    # Pass a JDBC URL into the Spark driver so it connects to the
    # correct Postgres service inside the Docker Compose network.
    application_args=["--jdbc-url", jdbc_url],
    # Do not push large results to XCom
    do_xcom_push=False,
  )

  # replaced by a SparkSubmitOperator named the same so choose_job can remain unchanged
  # Use client deploy mode (default) for spark-submit on standalone clusters.
  # Standalone Spark does not support cluster deploy for python applications
  # (see logs: "Cluster deploy mode is currently not supported for python
  # applications on standalone clusters"). Keep the python3 prefs so Spark
  # will prefer a python3 binary when available, but you must ensure the
  # driver's Python minor version matches the executors' (otherwise
  # PySpark errors with [PYTHON_VERSION_MISMATCH]). Typical fixes:
  #  - upgrade the Airflow container to Python 3.10 to match workers, OR
  #  - rebuild workers to use Python 3.8 to match the Airflow driver.
  run_new_matches_mongo = SparkSubmitOperator(
    task_id="run_new_matches_mongo",
    application="/opt/airflow/jobs/spark_mongoNewMatches.py",
    conn_id="spark_default",
    packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
    application_args=["{{ ti.xcom_pull(task_ids='push_webhook_xcom') }}"],
    driver_memory="1g",
    executor_memory="2g",
    # Leave deploy_mode unset (client) for standalone clusters.
    conf={
      "spark.pyspark.python": "python3",
      "spark.executorEnv.PYSPARK_PYTHON": "python3",
      "spark.pyspark.driver.python": "python3",
    },
  )

  join_postgres = EmptyOperator(task_id="join_postgres", trigger_rule="one_success")

  join_mongo = EmptyOperator(task_id="join_mongo", trigger_rule="one_success")

  end_task = end_msg()

  # Connect tasks/operators
  start_task >> spark_smoke
  # run the spark smoke test first, then continue to the postgres branch
  spark_smoke >> postgres_branch_task
  postgres_branch_task >> [new_basho_postgres, end_basho_postgres, new_matches_postgres, match_results_postgres, skip_postgres]
  [new_basho_postgres, end_basho_postgres, new_matches_postgres, match_results_postgres, skip_postgres] >> join_postgres
  # Run homepage and the mongo branch in parallel after postgres join
  join_postgres >> homepage_task
  # Decide whether the mongo branch should run. Place the branch operator
  # immediately after the postgres join so it can pick between the two
  # downstream paths:
  #  - push_webhook_xcom -> run_new_matches_mongo -> join_mongo
  #  - skip_jobs_mongo -> join_mongo
  # This ensures only one of run_new_matches_mongo or skip_jobs_mongo runs.
  join_postgres >> mongo_branch_task

  # Branch downstream choices
  mongo_branch_task >> [push_webhook_xcom, skip_mongo]

  # When chosen, push webhook into XCom then run the Spark job, else skip.
  push_webhook_xcom >> run_new_matches_mongo >> join_mongo

  # Both homepage and the mongo branch must finish before finishing the DAG
  homepage_task >> join_mongo
  join_mongo >> end_task
