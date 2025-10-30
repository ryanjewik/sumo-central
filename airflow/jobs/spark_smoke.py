from pyspark.sql import SparkSession
import os


def _executor_check_status(rows):
    """Run on executor partition and return a concise status dict.

    The check does two things:
      - Mongo: import pymongo, read MONGO_URI / MONGO_DB_NAME / MONGO_COLL_NAME,
        perform a single upsert to confirm connector/pymongo works.
      - Postgres: import psycopg2, read DB_HOST / DB_NAME (and optional
        DB_PORT/DB_USERNAME/DB_PASSWORD), run SELECT 1.

    The function returns an iterator with a single status dict so the driver
    can collect and log results.
    """
    status = {"mongo_ok": False, "mongo_msg": None, "pg_ok": False, "pg_msg": None}

    # Mongo check
    try:
        from pymongo import MongoClient
    except Exception as e:
        status["mongo_msg"] = f"pymongo import failed: {e}"
    else:
        uri = os.environ.get("MONGO_URI")
        db_name = os.environ.get("MONGO_DB_NAME")
        coll_name = os.environ.get("MONGO_COLL_NAME")
        if not uri or not db_name or not coll_name:
            status["mongo_msg"] = "MONGO_URI / MONGO_DB_NAME / MONGO_COLL_NAME must be set in executor environment"
        else:
            try:
                client = MongoClient(uri)
                try:
                    coll = client.get_database(db_name)[coll_name]
                    # perform a harmless upsert
                    coll.replace_one({"_smoke": True}, {"_smoke": True}, upsert=True)
                    status["mongo_ok"] = True
                    status["mongo_msg"] = "Mongo upsert succeeded"
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass
            except Exception as e:
                status["mongo_msg"] = f"Mongo operation failed: {e}"
                raise

    # Postgres check
    try:
        import psycopg2
    except Exception as e:
        status["pg_msg"] = f"psycopg2 import failed: {e}"
    else:
        pg_host = os.environ.get("DB_HOST")
        pg_db = os.environ.get("DB_NAME")
        if not pg_host or not pg_db:
            status["pg_msg"] = "DB_HOST and DB_NAME must be set in executor environment"
        else:
            try:
                pg_port = int(os.environ.get("DB_PORT")) if os.environ.get("DB_PORT") else 5432
                pg_user = os.environ.get("DB_USERNAME")
                pg_pwd = os.environ.get("DB_PASSWORD")
                conn_args = {"host": pg_host, "port": pg_port, "dbname": pg_db}
                if pg_user:
                    conn_args["user"] = pg_user
                if pg_pwd:
                    conn_args["password"] = pg_pwd
                conn = psycopg2.connect(**conn_args)
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT 1")
                    res = cur.fetchone()
                    status["pg_ok"] = True
                    status["pg_msg"] = f"SELECT 1 returned: {res}"
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception as e:
                status["pg_msg"] = f"Postgres check failed: {e}"
                raise

    # --- S3 check (executor-side) ---
    # NOTE: Creating SparkSession or referencing SparkContext on workers can raise
    # CONTEXT_ONLY_VALID_ON_DRIVER errors. Use boto3 in the executor to validate
    # S3 connectivity and credentials from the executor environment instead.
        # --- S3 check (simplified) ---
    try:
        import uuid
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError

        s3_path = os.environ.get("S3_SMOKE_PATH")
        if not s3_path:
            status["s3_ok"] = False
            status["s3_msg"] = "S3_SMOKE_PATH not set on executor"
        else:
            # normalize s3a:// -> s3://
            if s3_path.startswith("s3a://"):
                s3_path = "s3://" + s3_path[len("s3a://"):]
            if not s3_path.startswith("s3://"):
                status["s3_ok"] = False
                status["s3_msg"] = f"Invalid S3 path: {s3_path}"
            else:
                without_scheme = s3_path[len("s3://"):].lstrip("/")
                parts = without_scheme.split("/", 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ""
                key = (prefix.rstrip("/") + f"/executors/{uuid.uuid4()}/smoke.txt").lstrip("/")

                region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

                # record basic env presence (no secrets)
                status["s3_env"] = {
                    "s3_path_used": s3_path,
                    "region_env": bool(region),
                    "aws_key_present": bool(os.environ.get("AWS_ACCESS_KEY_ID")),
                    "aws_secret_present": bool(os.environ.get("AWS_SECRET_ACCESS_KEY")),
                }

                # build client – rely on container creds
                if region:
                    s3 = boto3.client("s3", region_name=region)
                else:
                    s3 = boto3.client("s3")

                try:
                    s3.put_object(Bucket=bucket, Key=key, Body=b"ok")
                    s3.head_object(Bucket=bucket, Key=key)
                    status["s3_ok"] = True
                    status["s3_msg"] = f"Put/head succeeded at s3://{bucket}/{key}"
                except NoCredentialsError:
                    status["s3_ok"] = False
                    status["s3_msg"] = "No AWS credentials found in executor environment"
                except ClientError as ce:
                    # just surface it; no multi-region retry here
                    status["s3_ok"] = False
                    status["s3_msg"] = f"S3 client error: {ce}"
    except Exception as e:
        status["s3_ok"] = False
        status["s3_msg"] = f"S3 check failed to run: {e}"


    return iter([status])


if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_smoke").getOrCreate()
    sc = spark.sparkContext

    print("✅ Spark started")
    print("Count:", spark.range(0, 10).count())

    print("[driver] submitting executor checks (single partition)")
    try:
        results = sc.parallelize([0], 1).mapPartitions(_executor_check_status).collect()
        print("[driver] executor check results:")
        for r in results:
            print(r)
        print("[driver] executor checks completed")
    except Exception as e:
        print(f"[driver] executor checks job failed: {e}")

    spark.stop()
