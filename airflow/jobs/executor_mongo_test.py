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
        raise RuntimeError("MONGO_URI must be provided in executor environment for executor_mongo_test")

    client = MongoClient(uri)
    try:
        db_name = os.environ.get("MONGO_DB_NAME")
        coll_name = os.environ.get("MONGO_COLL_NAME")
        if not db_name or not coll_name:
            raise RuntimeError("MONGO_DB_NAME and MONGO_COLL_NAME must be set in executor environment for executor_mongo_test")
        db = client.get_database(db_name)
        coll = db[coll_name]
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
