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
