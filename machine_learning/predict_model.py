from pyspark.sql import SparkSession, functions as F
from dotenv import load_dotenv
import os, pathlib, shutil, sys
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sklearn
import xgboost.sklearn
from xgboost.spark import SparkXGBClassifier
import re
from pyspark.ml.feature import Imputer
from pyspark.sql import functions as F, Window
from pyspark.sql.types import NumericType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from datetime import datetime



os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"]       = r"C:\hadoop\bin;" + os.environ["PATH"]
os.environ["AWS_PROFILE"] = "ryanj"
os.environ["AWS_REGION"]  = "us-west-2"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = rf'{os.environ["HADOOP_HOME"]}\bin;' + os.environ["PATH"]
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.aws.region=us-west-2 "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain "
    "--conf spark.hadoop.io.native.lib.available=false "
    "--conf spark.hadoop.fs.s3a.fast.upload=true "
    "--conf spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer "
    "pyspark-shell"
)


load_dotenv()


#Spark configuration--------------------------------------------

spark = (SparkSession.builder
         .appName("sumo-s3a")
         .config(
            "spark.jars.packages",
            "org.xgboost:xgboost4j_2.12:2.0.3,"
            "org.xgboost:xgboost4j-spark_2.12:2.0.3,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
         )
         # make sure S3A is wired up; you can keep these if they helped earlier
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
         .config("spark.hadoop.fs.s3a.aws.region", "us-west-2")
         .config("spark.driver.memory", "4g")
         .config("spark.executor.memory", "4g")
         .config("spark.pyspark.python", sys.executable)          # workers use venv python
         .config("spark.pyspark.driver.python", sys.executable)   # driver uses venv python
         .config("spark.network.timeout", "600s")
         .config("spark.executor.heartbeatInterval", "60s")
         .config("spark.sql.execution.barrier.enabled", "true")
         
         .getOrCreate())


spark_local = r"C:\tmp\spark"
pathlib.Path(spark_local).mkdir(parents=True, exist_ok=True)

spark.conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")   # 30s
spark.conf.set("spark.hadoop.fs.s3a.threads.keepalivetime",        "60000")   # 60s
spark.conf.set("spark.hadoop.fs.s3a.connection.ttl",              "300000")   # 5m
spark.conf.set("spark.hadoop.fs.s3a.retry.interval",                 "500")   # 500ms
spark.conf.set("spark.hadoop.fs.s3a.retry.throttle.interval",        "100")   # 100ms

hconf = spark.sparkContext._jsc.hadoopConfiguration()

fixes = {
    "fs.s3a.connection.establish.timeout": "30000",    # 30s
    "fs.s3a.threads.keepalivetime": "60000",           # 60s
    "fs.s3a.retry.interval": "500",                    # 500ms
    "fs.s3a.retry.throttle.interval": "100",           # 100ms
    "fs.s3a.connection.ttl": "300000",                 # 5m
    "fs.s3a.assumed.role.session.duration": "1800000"  # 30m
}
for k, v in fixes.items():
    hconf.set(k, v)


hconf = spark.sparkContext._jsc.hadoopConfiguration()

# report any time-looking values with units
offenders = []
it = hconf.iterator()
pairs = []
while it.hasNext():
    e = it.next()
    pairs.append((e.getKey(), e.getValue()))

r_time = re.compile(r"^\s*\d+\s*(ms|s|m|h|d)\s*$", re.I)

for k, v in pairs:
    if r_time.match(str(v)):
        offenders.append((k, v))

print("Offenders before fix:")
for k, v in offenders:
    print(f"  {k} = {v}")

# convert unit-suffixed durations to milliseconds
UNIT_MS = {"ms":1, "s":1000, "m":60_000, "h":3_600_000, "d":86_400_000}

def to_ms_str(val):
    m = re.fullmatch(r"\s*(\d+)\s*(ms|s|m|h|d)\s*", str(val).lower())
    if not m: 
        return None
    n = int(m.group(1)); u = m.group(2)
    return str(n * UNIT_MS[u])

changed = []
for k, v in pairs:
    ms = to_ms_str(v)
    if ms and ms != v:
        hconf.set(k, ms)
        changed.append((k, v, ms))

print("\nNormalized to milliseconds:")
for k, old, new in changed:
    print(f"  {k}: {old} -> {new}")

# show any stragglers still using units
left = []
it = hconf.iterator()
while it.hasNext():
    e = it.next()
    if r_time.match(str(e.getValue())):
        left.append((e.getKey(), e.getValue()))
print("\nRemaining with units (should be empty):", left)

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")  # avoids choking on stray files



#Data cleaning and wrangling--------------------------------------------

main = (spark.read              # set to "false" if you have JSON Lines
      .option("mode", "PERMISSIVE")               # or "FAILFAST"
      .option("recursiveFileLookup", "true")
      .parquet("s3a://ryans-sumo-bucket/gold/ml_training/"))


main = main.withColumn(
    "match_id",
    concat(
        col("match_id").cast(StringType()),
        col("eastId").cast(StringType()),
        col("westId").cast(StringType())
    )
)


#ROLLING AVERAGES--------------------------------------------
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

#drop and fill null values
main = main.drop('winnerEn', 'winnerJp', 'division', 'day', 'matchNo', 'west_intai', 'east_intai', 'west_rankValue', 'east_rankValue', 'eastRank', 'westRank', 'eastShikona', 'westShikona', 'east_birthdate_parsed', 'west_birthdate_parsed', 'winnerId', 'west_id', 'east_id', 'west_debut', 'east_debut', 'west_debut_parsed', 'east_debut_parsed', 'east_debut_clean', 'west_debut_clean', 'west_rikishi_id', 'east_rikishi_id', 'west_currentRank', 'east_currentRank', 'west_rank', 'east_rank', 'match_date')

imputer = Imputer(inputCols = ['west_height', 'west_weight', 'west_age', 'east_height', 'east_weight', 'east_age'], 
                  outputCols = ['west_height', 'west_weight', 'west_age', 'east_height', 'east_weight', 'east_age']
                 ).setStrategy("mean")
main = imputer.fit(main).transform(main)
main = main.replace("", "NA", subset=['kimarite'])
main = main.na.fill(value = main.select(max(col("west_order"))).first()[0], subset=["west_order", "east_order"])

#WINRATE FEATURES--------------------------------------------
def safe_ratio(numer, denom, default=F.lit(None)):
    return F.when(denom.isNull() | (denom == 0), default).otherwise(numer / denom)

main = (
    main
    .withColumn("west_winRate", safe_ratio(F.col("west_totalWins").cast("double"), F.col("west_totalMatches").cast("double")))
    .withColumn("east_winRate", safe_ratio(F.col("east_totalWins").cast("double"), F.col("east_totalMatches").cast("double")))
    .withColumn("west_makuuchiWinRate", safe_ratio(F.col("west_makuuchiWins").cast("double"), F.col("west_Makuuchi_basho").cast("double")))
    .withColumn("east_makuuchiWinRate", safe_ratio(F.col("east_makuuchiWins").cast("double"), F.col("east_Makuuchi_basho").cast("double")))
)
#HEIGHT AND WEIGHT DIFFERENCE FEATURES--------------------------------------------
main = (main
        .withColumn("height_difference", F.col("west_height") - F.col("east_height"))
        .withColumn("weight_difference", F.col("west_weight") - F.col("east_weight"))
       )

#KIMARITE FEATURES--------------------------------------------
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
                 "east_makuuchi_yusho", "west_height", "west_weight", "east_height", "east_weight", "kimarite", "eastId", "westId", "west_specialist", "east_specialist",
                 "west_vs_oshi_winrate", "west_vs_yotsu_winrate", "west_vs_other_winrate", "east_vs_oshi_winrate", "east_vs_yotsu_winrate", "east_vs_other_winrate",
                )
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
(main.write
 .mode("overwrite")
 .option("compression", "snappy")
 .parquet(f"s3a://ryans-sumo-bucket/gold/ml_training/"))

#MACHINE LEARNING MODEL--------------------------------------------

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
cv_model = cv.fit(train)
preds = cv_model.transform(test)

print("AUC:", evaluator.evaluate(preds))
acc = preds.filter(preds.westWin == preds.prediction).count() / float(preds.count())
print("Accuracy:", acc)


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
path = f"s3a://ryans-sumo-bucket/models/sumo_westwin_best_pipeline/xgboost_tree/{timestamp}"

best_pipeline_model = cv_model.bestModel
best_pipeline_model.save(path)


from pyspark.ml import PipelineModel

loaded_best_pipeline = PipelineModel.load(path)