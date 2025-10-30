from __future__ import annotations

import argparse
import logging
import os
from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml.feature import Imputer
from pyspark.sql.types import NumericType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from xgboost.spark import SparkXGBClassifier


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")



def create_spark_session(
    app_name: str = "data_cleaning_job",
    driver_memory: Optional[str] = "4g",
    executor_memory: Optional[str] = "4g",
) -> SparkSession:
    driver_mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY")
    executor_mem = executor_memory or os.environ.get("SPARK_EXECUTOR_MEMORY")

    builder = (
        SparkSession.builder.appName(app_name)
        # we assume the image already has hadoop-aws + aws-java-sdk
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )

    # optional: let container env decide region, not this script
    # if you really want to force:
    # builder = builder.config("spark.hadoop.fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    if driver_mem:
        LOG.info("Setting spark.driver.memory=%s", driver_mem)
        builder = builder.config("spark.driver.memory", driver_mem)
    if executor_mem:
        LOG.info("Setting spark.executor.memory=%s", executor_mem)
        builder = builder.config("spark.executor.memory", executor_mem)

    spark = builder.getOrCreate()

    # Hadoop-level S3A config (lightweight)
    hadoop_conf = spark._jsc.hadoopConfiguration()

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_token = os.environ.get("AWS_SESSION_TOKEN")

    # only set creds here if present; otherwise let the provider chain (instance role, etc.) handle it
    if aws_key and aws_secret:
        LOG.info("Applying AWS credentials to Hadoop config from environment")
        hadoop_conf.set("fs.s3a.access.key", aws_key)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret)
        if aws_token:
            hadoop_conf.set("fs.s3a.session.token", aws_token)

    # good defaults, but not overkill
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.fast.upload", "true")

    return spark


def process_ml_training(
    input_path: str,
    output_path: str,
    input_format: str = "parquet",
    output_format: str = "parquet",
    partition_by: Optional[list[str]] = None,
    dropna_thresh: Optional[int] = None,
    app_name: str = "data_cleaning_job",
    driver_memory: Optional[str] = None,
    executor_memory: Optional[str] = None,
) -> None:
    LOG.info("Starting process_data: %s -> %s", input_path, output_path)
    spark = create_spark_session(app_name=app_name, driver_memory=driver_memory, executor_memory=executor_memory)

    try:
        print("starting process")
        main = (spark.read              # set to "false" if you have JSON Lines
            .option("mode", "PERMISSIVE")               # or "FAILFAST"
            .option("recursiveFileLookup", "true")
            .parquet("s3a://ryans-sumo-bucket/gold/cleaned_data/"))
        print("read main data")
        main = main.withColumn(
            "match_id",
            concat(
                col("match_id").cast(StringType()),
                col("eastId").cast(StringType()),
                col("westId").cast(StringType())
            )
        )
        
        # Add unique match_id for joining back later
        main = (
            main
            .withColumn("match_ts", F.col("match_date").cast("timestamp"))
        )

        # get west_id rows
        west_rows = (
            main
            .select(
                "match_id", "match_ts",
                F.col("westId").alias("rikishiId"),
                F.lit("west").alias("side"),
                F.col("westWin").cast("int").alias("isWin")
            )
        )

        #get east_id rows
        east_rows = (
            main
            .select(
                "match_id", "match_ts",
                F.col("eastId").alias("rikishiId"),
                F.lit("east").alias("side"),
                (1 - F.col("westWin").cast("int")).alias("isWin")
            )
        )

        long_df = (
            west_rows.unionByName(east_rows)
            .withColumn("ts", F.col("match_ts").cast("long"))
        )

        # Define rolling 6-month window (~183 days)
        six_months_sec = 183 * 24 * 3600
        # this defines a "window" of rows that we will run our rolling average calculation over so it doesn't run on the whole dataset
        w6 = Window.partitionBy("rikishiId").orderBy("ts").rangeBetween(-six_months_sec, 0)

        # Rolling win/loss %
        rolled_long = (
            long_df
            .withColumn("wins_6m", F.sum("isWin").over(w6))
            .withColumn("games_6m", F.count(F.lit(1)).over(w6))
            #precision 2 using wins_6m and games_6m
            .withColumn("rolling_winloss", F.round(F.col("wins_6m") / F.col("games_6m"), 2))
        )

        # Pivot back to per match
        rolled_wide = (
            rolled_long
            .groupBy("match_id")
            .agg(
                F.first(F.when(F.col("side") == "west", F.col("rolling_winloss")), ignorenulls=True)
                .alias("rolling_winloss_west"),
                F.first(F.when(F.col("side") == "east", F.col("rolling_winloss")), ignorenulls=True)
                .alias("rolling_winloss_east")
            )
        )

        # Merge back into main
        main = (
            main
            .join(rolled_wide, on="match_id", how="left")
            .drop("match_id", "match_ts")
        )
        
        main = main.drop('winnerEn', 'winnerJp', 'division', 'day', 'matchNo', 'west_intai', 'east_intai', 'west_rankValue', 'east_rankValue', 'eastRank', 'westRank', 'eastShikona', 'westShikona', 'east_birthdate_parsed', 'west_birthdate_parsed', 'winnerId', 'west_id', 'east_id', 'west_debut', 'east_debut', 'west_debut_parsed', 'east_debut_parsed', 'east_debut_clean', 'west_debut_clean', 'west_rikishi_id', 'east_rikishi_id', 'west_currentRank', 'east_currentRank', 'west_rank', 'east_rank', 'match_date')
        
        
        
        imputer = Imputer(inputCols = ['west_height', 'west_weight', 'west_age', 'east_height', 'east_weight', 'east_age'], 
                        outputCols = ['west_height', 'west_weight', 'west_age', 'east_height', 'east_weight', 'east_age']
                        ).setStrategy("mean")
        main = imputer.fit(main).transform(main)
        
        main = main.replace("", "NA", subset=['kimarite'])
        
        main = main.na.fill(value = main.select(max(col("west_order"))).first()[0], subset=["west_order", "east_order"])
        
        def safe_ratio(numer, denom, default=F.lit(None)):
            return F.when(denom.isNull() | (denom == 0), default).otherwise(numer / denom)

        # 3) Core win-rate features for both sides
        main = (
            main
            .withColumn("west_winRate", safe_ratio(F.col("west_totalWins").cast("double"), F.col("west_totalMatches").cast("double")))
            .withColumn("east_winRate", safe_ratio(F.col("east_totalWins").cast("double"), F.col("east_totalMatches").cast("double")))
            .withColumn("west_makuuchiWinRate", safe_ratio(F.col("west_makuuchiWins").cast("double"), F.col("west_Makuuchi_basho").cast("double")))
            .withColumn("east_makuuchiWinRate", safe_ratio(F.col("east_makuuchiWins").cast("double"), F.col("east_Makuuchi_basho").cast("double")))
        )
        main = (main
            .withColumn("height_difference", F.col("west_height") - F.col("east_height"))
            .withColumn("weight_difference", F.col("west_weight") - F.col("east_weight"))
        )
        
        yotsu_list = [
            "yorikiri","yoritaoshi","uwatenage","shitatenage","uwatedashinage",
            "shitatedashinage","tsuridashi","tsuriotoshi","kimedashi","kimetaoshi",
            "sotogake","uchigake","kakenage","kubinage","kotenage","tottari",
            "uwatehineri","shitatehineri","kainahineri","ashitori","susoharai",
            "susotori"
        ]

        oshi_list = [
            "oshidashi","oshitaoshi","tsukidashi","tsukiotoshi","tsukitaoshi",
            "hatakikomi","hikiotoshi","abisetaoshi","hikkake","okuridashi",
            "okuritaoshi","okurihikiotoshi"
        ]
        
        def cat_expr(k):
            return (F.when(k.isin(yotsu_list), F.lit("yotsu"))
                    .when(k.isin(oshi_list), F.lit("oshi"))
                    .otherwise(F.lit("other")))

        # 2) Winner-normalized rows; drop NA/unknown kimarite from specialization stats
        main_winners = (
            main
            .withColumn("winnerId", F.when(F.col("westWin") == 1, F.col("westId")).otherwise(F.col("eastId")))
            .withColumn("kimarite_norm",
                        F.when(F.col("kimarite").isNull() | (F.col("kimarite") == F.lit("NA")), F.lit(None))
                        .otherwise(F.col("kimarite")))
            .filter(F.col("kimarite_norm").isNotNull())
            .withColumn("category", cat_expr(F.col("kimarite_norm")))
        )

        # 3) Per-rikishi counts & proportions (pivot)
        counts = main_winners.groupBy("winnerId","category").count()
        pivot = (
            counts.groupBy("winnerId")
                .pivot("category", ["oshi","yotsu","other"])
                .sum("count")
                .na.fill(0)
                .withColumn("total", F.col("oshi")+F.col("yotsu")+F.col("other"))
                .withColumn("oshi_pct",  F.when(F.col("total")>0, F.col("oshi") / F.col("total")).otherwise(F.lit(0.0)))
                .withColumn("yotsu_pct", F.when(F.col("total")>0, F.col("yotsu")/ F.col("total")).otherwise(F.lit(0.0)))
                .withColumn("other_pct", F.when(F.col("total")>0, F.col("other")/ F.col("total")).otherwise(F.lit(0.0)))
        )

        # 4) Kimarite diversity (Shannon entropy) in pure Spark:
        # entropy = -Σ p*ln(p); treat p<=0 as 0 contribution
        def entropy_expr(o, y, ot):
            def term(p):  # ln is natural log
                return F.when(p > 0, p * F.log(p)).otherwise(F.lit(0.0))
            return -(term(o) + term(y) + term(ot))

        pivot = pivot.withColumn("kimarite_entropy", entropy_expr(F.col("oshi_pct"), F.col("yotsu_pct"), F.col("other_pct")))

        # 5) Specialist label (tune thresholds if you’d like)
        pivot = pivot.withColumn(
            "specialist",
            F.when(
                (F.col("oshi_pct") > F.col("yotsu_pct")) & (F.col("oshi_pct") > F.col("other_pct")),
                F.lit("oshi")
            ).when(
                (F.col("yotsu_pct") > F.col("oshi_pct")) & (F.col("yotsu_pct") > F.col("other_pct")),
                F.lit("yotsu")
            ).otherwise(F.lit("other"))   # ties or 'other' majority fall here
        )

        rikishi_profile = pivot.select(
            F.col("winnerId").alias("rikishiId"),
            "specialist",
            "kimarite_entropy",
        )

        # 6) Join back to main (west/east) and make binary flags
        main = (
            main
            .join(
                rikishi_profile.withColumnRenamed("specialist","west_specialist")
                            .withColumnRenamed("kimarite_entropy","west_kimarite_entropy"),
                on=main.westId == F.col("rikishiId"), how="left"
            ).drop("rikishiId")
            .join(
                rikishi_profile.withColumnRenamed("specialist","east_specialist")
                            .withColumnRenamed("kimarite_entropy","east_kimarite_entropy"),
                on=main.eastId == F.col("rikishiId"), how="left"
            ).drop("rikishiId")
            .withColumn("west_specialist_oshi",  (F.col("west_specialist")=="oshi").cast("int"))
            .withColumn("west_specialist_yotsu", (F.col("west_specialist")=="yotsu").cast("int"))
            .withColumn("west_specialist_other", (F.col("west_specialist")=="other").cast("int"))
            .withColumn("east_specialist_oshi",  (F.col("east_specialist")=="oshi").cast("int"))
            .withColumn("east_specialist_yotsu", (F.col("east_specialist")=="yotsu").cast("int"))
            .withColumn("east_specialist_other", (F.col("east_specialist")=="other").cast("int"))
        )
        
        west_view = (
            main.select(
                F.col("westId").alias("rikishiId"),
                F.col("east_specialist").alias("opponent_specialist"),
                F.when(F.col("westWin") == 1, 1).otherwise(0).alias("win")
            )
        )

        east_view = (
            main.select(
                F.col("eastId").alias("rikishiId"),
                F.col("west_specialist").alias("opponent_specialist"),
                F.when(F.col("westWin") == 0, 1).otherwise(0).alias("win")
            )
        )

        rikishi_vs = west_view.union(east_view)

        winrates = (
            rikishi_vs.groupBy("rikishiId", "opponent_specialist")
                    .agg(
                        F.count("*").alias("bouts"),
                        F.sum("win").alias("wins")
                    )
                    .withColumn("winrate", F.when(F.col("bouts") > 0, F.col("wins")/F.col("bouts")).otherwise(F.lit(None)))
        )

        # 2) Pivot so each rikishi has columns: oshi, yotsu, other (their winrate vs that class)
        winrates_pivot = (
            winrates.groupBy("rikishiId")
                    .pivot("opponent_specialist", ["oshi","yotsu","other"])
                    .agg(F.first("winrate"))
            # don't fill with 0 by default—keep nulls when they have no history vs that class
        )

        # 3) Join back to every bout for WEST perspective
        west_wr = (
            winrates_pivot
            .withColumnRenamed("oshi",  "west_vs_oshi_winrate")
            .withColumnRenamed("yotsu", "west_vs_yotsu_winrate")
            .withColumnRenamed("other", "west_vs_other_winrate")
        )

        with_west = (
            main.join(west_wr, main.westId == west_wr.rikishiId, "left")
                    .drop(west_wr.rikishiId)
        )

        # 4) Join back to every bout for EAST perspective
        east_wr = (
            winrates_pivot
            .withColumnRenamed("oshi",  "east_vs_oshi_winrate")
            .withColumnRenamed("yotsu", "east_vs_yotsu_winrate")
            .withColumnRenamed("other", "east_vs_other_winrate")
        )

        with_both = (
            with_west.join(east_wr, with_west.eastId == east_wr.rikishiId, "left")
                    .drop(east_wr.rikishiId)
        )

        # 5) Convenience columns: per-row winrate vs *this bout’s opponent’s class*
        main = (
            with_both
            .withColumn(
                "west_winrate_vs_opponent_specialist",
                F.when(F.col("east_specialist") == "oshi",  F.col("west_vs_oshi_winrate"))
                .when(F.col("east_specialist") == "yotsu", F.col("west_vs_yotsu_winrate"))
                .when(F.col("east_specialist") == "other", F.col("west_vs_other_winrate"))
                .otherwise(F.lit(None))
            )
            .withColumn(
                "east_winrate_vs_opponent_specialist",
                F.when(F.col("west_specialist") == "oshi",  F.col("east_vs_oshi_winrate"))
                .when(F.col("west_specialist") == "yotsu", F.col("east_vs_yotsu_winrate"))
                .when(F.col("west_specialist") == "other", F.col("east_vs_other_winrate"))
                .otherwise(F.lit(None))
            )
        )
        
        main = main.na.fill(value=0)
        main = main.drop('west_totalWins', 'west_totalMatches', 'west_totalLosses', 'east_totalWins', 'east_totalMatches', 'east_totalLosses', 
                        'west_basho', 'east_basho', 'west_age', 'east_age', "east_Gino_sho", "east_Kanto_sho", "east_Shukun_sho","west_Gino_sho",
                            "west_Kanto_sho", "west_Shukun_sho", "west_makuuchi_yusho",
                        "east_makuuchi_yusho", "west_height", "west_weight", "east_height", "east_weight", "kimarite",  "west_specialist", "east_specialist",
                        "west_vs_oshi_winrate", "west_vs_yotsu_winrate", "west_vs_other_winrate", "east_vs_oshi_winrate", "east_vs_yotsu_winrate", "east_vs_other_winrate",
                        )
        
        (main.write
         .mode("overwrite")
         .option("compression", "snappy")
         .parquet("s3a://ryans-sumo-bucket/gold/ml_training_set/"))
        print("wrote ml training dataset")
        print("prepared main data for ML")
        #starting ML training
        
        main = main.drop('westId', 'eastId')
        
        feature_cols = [
            "west_yusho",
            "west_makuuchiWins", "west_Makuuchi_basho",
            "east_yusho", "east_makuuchiWins",
            "east_Makuuchi_basho", "west_order", "east_order", "east_years_active",
            "west_years_active", "rolling_winloss_west", "rolling_winloss_east",
            "west_winRate", "east_winRate", "west_makuuchiWinRate",
            "east_makuuchiWinRate", "height_difference", "weight_difference", "west_kimarite_entropy", "east_kimarite_entropy", "west_specialist_oshi",
            "east_specialist_oshi", "west_specialist_yotsu", "east_specialist_yotsu", "west_specialist_other", "east_specialist_other", "west_winrate_vs_opponent_specialist",
            "east_winrate_vs_opponent_specialist"
        ]
        
        
        # Assemble features
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        # Define XGBoost model
        xgb = SparkXGBClassifier(
            features_col="features",
            label_col="westWin",
            prediction_col="prediction",
            probability_col="probability",
            eval_metric="auc",
            max_depth=6,
            eta=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            num_workers=8,
            missing = -999.0
        )

        # Build pipeline
        pipeline = Pipeline(stages=[assembler, xgb])
        
        paramGrid = (
            ParamGridBuilder()
            .addGrid(xgb.max_depth, [4, 6, 8])
            .addGrid(xgb.learning_rate, [0.05, 0.1, 0.2])
            .addGrid(xgb.subsample, [0.8, 1.0])
            .addGrid(xgb.colsample_bytree, [0.8, 1.0])
            .build()
        )

        evaluator = BinaryClassificationEvaluator(
            labelCol="westWin",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            numFolds=3,      # consider 3 folds locally; XGB can be heavy
            parallelism=2
        )
        train, test = main.randomSplit([0.8, 0.2], seed=42)
        print("training model...")
        cv_model = cv.fit(train)
        preds = cv_model.transform(test)
        print("AUC:", evaluator.evaluate(preds))
        acc = preds.filter(preds.westWin == preds.prediction).count() / float(preds.count())
        print("Accuracy:", acc)
        
        PRED_DF = preds  # <- replace with your DF that has ground truth + predictions
        LABEL_COL = "westWin"        # ground-truth: 1 = west won, 0 = east won
        PRED_COL  = "prediction"     # model prediction in {0,1}

        # Confusion matrix table (pivoted)
        cm = (
            PRED_DF
            .select(F.col(LABEL_COL).cast("int").alias("y"),
                    F.col(PRED_COL).cast("int").alias("yhat"))
            .groupBy("y","yhat").count()
            .groupBy("y")
            .pivot("yhat", [0,1])  # columns: pred=0, pred=1
            .sum("count")
            .na.fill(0)
            .orderBy("y")
        )
        # cm schema: y, 0, 1  (rows: actual 0 then 1; cols: pred 0, pred 1)

        # Pull named cells: TN, FP, FN, TP
        cells = (
            PRED_DF.select(
                F.sum(F.when((F.col(LABEL_COL)==0) & (F.col(PRED_COL)==0), 1).otherwise(0)).alias("TN"),
                F.sum(F.when((F.col(LABEL_COL)==0) & (F.col(PRED_COL)==1), 1).otherwise(0)).alias("FP"),
                F.sum(F.when((F.col(LABEL_COL)==1) & (F.col(PRED_COL)==0), 1).otherwise(0)).alias("FN"),
                F.sum(F.when((F.col(LABEL_COL)==1) & (F.col(PRED_COL)==1), 1).otherwise(0)).alias("TP"),
            )
        )

        metrics = (
            cells.select(
                "TP","FP","FN","TN",
                ((F.col("TP")+F.col("TN")) / (F.col("TP")+F.col("TN")+F.col("FP")+F.col("FN"))).alias("accuracy"),
                (F.col("TP") / F.when((F.col("TP")+F.col("FP"))>0, F.col("TP")+F.col("FP")).otherwise(None)).alias("precision_pos"),
                (F.col("TP") / F.when((F.col("TP")+F.col("FN"))>0, F.col("TP")+F.col("FN")).otherwise(None)).alias("recall_pos"),
                (F.col("TN") / F.when((F.col("TN")+F.col("FP"))>0, F.col("TN")+F.col("FP")).otherwise(None)).alias("specificity"),
                (2*F.col("TP") / F.when((2*F.col("TP")+F.col("FP")+F.col("FN"))>0, 2*F.col("TP")+F.col("FP")+F.col("FN")).otherwise(None)).alias("f1_pos"),
            )
        )

        # Show
        cm.show(truncate=False)
        metrics.show(truncate=False)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"s3a://ryans-sumo-bucket/models/xgboost_model"
        best_pipeline_model = cv_model.bestModel
        best_pipeline_model.save(path)
        print("Model saved to: %s" % path)
        
        

    except Exception as e:
        LOG.error("Error during process_data: %s", e)
        print("error during process_data    %s" % e)
        raise

    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("Simple Spark ml training job")
    p.add_argument("--input", default="s3a://ryans-sumo-bucket/sumo-api-calls/rikishi_matches/")
    p.add_argument("--output", default="s3a://ryans-sumo-bucket/silver/rikishi_matches/")
    p.add_argument("--app-name", default="data_training_job")
    p.add_argument("--driver-memory")
    p.add_argument("--executor-memory")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    process_ml_training(
        input_path=args.input,
        output_path=args.output,
        app_name=args.app_name,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
    )




