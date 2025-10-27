from pyspark.sql import SparkSession
import os


def _write_partition(rows):
    try:
        from pymongo import MongoClient
    except Exception as e:
        # pymongo missing on executor
        print(f"pymongo import failed on executor: {e}")
        raise

    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("No MONGO_URI in environment on executor")
        return

    client = MongoClient(uri)
    try:
        db = client.get_database(os.environ.get("MONGO_DB_NAME") or "sumo")
        coll = db[os.getenv("MONGO_COLL_NAME") or "homepage"]
        for _ in rows:
            doc = {"_homepage_doc": True, "test": "executor_write", "ts": __import__("datetime").datetime.utcnow().isoformat()}
            coll.replace_one({"_homepage_doc": True}, doc, upsert=True)
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    spark = SparkSession.builder.appName("executor_mongo_test").getOrCreate()
    sc = spark.sparkContext
    sc.parallelize([0], 1).foreachPartition(_write_partition)
    print("Executor write job submitted")
