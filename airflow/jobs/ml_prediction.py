"""
first retrieve the model from s3, load it into memory, then prepare the data as needed to pass into the model for prediction

will need to, get the rikishi ids from the webhook, then find the rows from the pyspark dataframe to construct a row built with the two (westId and eastId)

if the rikishi id doesn't exist then vote for the other. if neither exist 50-50 it OR we can create a dataframe row using averages of division and height/weight etc

return the predictions back to the sparkNewMatches job to then be stored as a key value in mongo

"""

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col
from pyspark.ml.feature import Imputer
import logging

logger = logging.getLogger(__name__)

model_path = "s3a://ryans-sumo-bucket/models/xgboost_model"
# Don't load the model at import time; load on-demand or accept a preloaded model via
# the `model` parameter to avoid heavyweight loads during module import or inside executors.



def _find_id_column(cols, side_keyword):
    # try to find explicit id column names (westId, west_rikishi, etc.)
    lower = [c.lower() for c in cols]
    candidates = []
    for idx, c in enumerate(cols):
        cn = c.lower()
        if side_keyword in cn and ("id" in cn or "rikishi" in cn or "name" in cn):
            candidates.append(c)
    return candidates[0] if candidates else None


def match_predict(westId, eastId, spark_session=None, model=None, cleaned_path="s3a://ryans-sumo-bucket/gold/cleaned_data/"):
    """Build features for the two rikishi from `cleaned_data` and return model prediction.

    This function mirrors the core feature-engineering steps from the training job
    but limits processing to rows relevant to the two rikishi (their history). It then
    selects the same `feature_cols` used for training and runs the pipeline model.

    Returns: 1 (west wins), 0 (west loses), -1 on error/missing data.
    """

    # Use the provided spark_session / model if available. If not, create/load once.
    spark_sess = spark_session or SparkSession.builder.getOrCreate()
    mdl = model or PipelineModel.load(model_path)

    logger.info("match_predict called with westId=%s eastId=%s cleaned_path=%s", westId, eastId, cleaned_path)

    if not westId and not eastId:
        logger.error("Both westId and eastId are empty or None")
        raise ValueError("At least one of westId or eastId must be provided")

    # Read cleaned_data from S3 (we need rows for both rikishi history)
    main_all = spark_sess.read.option("mode", "PERMISSIVE").option("recursiveFileLookup", "true").parquet(cleaned_path)

    # Keep only rows where either rikishi appears (on either side) so transforms run faster
    main = main_all.filter(
        (F.col("westId") == westId) | (F.col("eastId") == westId) | (F.col("westId") == eastId) | (F.col("eastId") == eastId)
    )

    count_rows = main.count()
    logger.debug("Filtered cleaned_data rows for ids: %s", count_rows)
    if count_rows == 0:
        logger.error("No rows found in cleaned_data for either rikishi: %s, %s", westId, eastId)
        raise LookupError(f"No rows found in cleaned_data for rikishi ids: {westId}, {eastId}")

    # add match_ts for ordering
    main = main.withColumn("match_ts", F.col("match_date").cast("timestamp"))

    # Build long_df for rolling stats per rikishi
    west_rows = main.select("match_ts", F.col("westId").alias("rikishiId"), F.lit("west").alias("side"), F.col("westWin").cast("int").alias("isWin"))
    east_rows = main.select("match_ts", F.col("eastId").alias("rikishiId"), F.lit("east").alias("side"), (1 - F.col("westWin").cast("int")).alias("isWin"))

    long_df = west_rows.unionByName(east_rows).withColumn("ts", F.col("match_ts").cast("long"))

    # rolling 6 months window
    six_months_sec = 183 * 24 * 3600
    w6 = Window.partitionBy("rikishiId").orderBy("ts").rangeBetween(-six_months_sec, 0)

    rolled_long = (
        long_df
        .withColumn("wins_6m", F.sum("isWin").over(w6))
        .withColumn("games_6m", F.count(F.lit(1)).over(w6))
        .withColumn("rolling_winloss", F.round(F.col("wins_6m") / F.col("games_6m"), 2))
    )

    # pivot back per match: find first value per side
    # We need an identifier per match; create one from match_ts + rikishi ids
    main = main.withColumn("match_id", concat(F.col("match_date").cast("string"), F.col("eastId").cast("string"), F.col("westId").cast("string")))

    rolled_wide = (
        rolled_long.withColumn("match_id", concat(F.col("ts").cast("string"), F.col("rikishiId")))
        .groupBy("match_id")
        .agg(
            F.first(F.when(F.col("side") == "west", F.col("rolling_winloss")), ignorenulls=True).alias("rolling_winloss_west"),
            F.first(F.when(F.col("side") == "east", F.col("rolling_winloss")), ignorenulls=True).alias("rolling_winloss_east"),
        )
    )

    # join rolled_wide back into main on a best-effort match_id (we'll join on the ts-based match_id)
    main = main.join(rolled_wide, on="match_id", how="left")

    # simple imputer for heights/weights if present (use mean)
    numeric_cols = [c for c in ["west_height", "west_weight", "west_age", "east_height", "east_weight", "east_age"] if c in main.columns]
    if numeric_cols:
        imputer = Imputer(inputCols=numeric_cols, outputCols=numeric_cols).setStrategy("mean")
        main = imputer.fit(main).transform(main)

    # safe ratio helper
    def safe_ratio(numer, denom, default=F.lit(None)):
        return F.when(denom.isNull() | (denom == 0), default).otherwise(numer / denom)

    if "west_totalWins" in main.columns and "west_totalMatches" in main.columns:
        main = main.withColumn("west_winRate", safe_ratio(F.col("west_totalWins").cast("double"), F.col("west_totalMatches").cast("double")))
    if "east_totalWins" in main.columns and "east_totalMatches" in main.columns:
        main = main.withColumn("east_winRate", safe_ratio(F.col("east_totalWins").cast("double"), F.col("east_totalMatches").cast("double")))

    if ("west_height" in main.columns) and ("east_height" in main.columns):
        main = main.withColumn("height_difference", F.col("west_height") - F.col("east_height"))
    if ("west_weight" in main.columns) and ("east_weight" in main.columns):
        main = main.withColumn("weight_difference", F.col("west_weight") - F.col("east_weight"))

    # Kimarite category mapping (simple copy from training script)
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
        return (F.when(k.isin(yotsu_list), F.lit("yotsu")).when(k.isin(oshi_list), F.lit("oshi")).otherwise(F.lit("other")))

    # winner-normalized rows for specialization
    main_winners = (
        main
        .withColumn("winnerId", F.when(F.col("westWin") == 1, F.col("westId")).otherwise(F.col("eastId")))
        .withColumn("kimarite_norm", F.when(F.col("kimarite").isNull() | (F.col("kimarite") == F.lit("")), F.lit(None)).otherwise(F.col("kimarite")))
        .filter(F.col("kimarite_norm").isNotNull())
        .withColumn("category", cat_expr(F.col("kimarite_norm")))
    )

    counts = main_winners.groupBy("winnerId", "category").count()
    pivot = (
        counts.groupBy("winnerId").pivot("category", ["oshi","yotsu","other"]).sum("count").na.fill(0)
    )

    def entropy_expr(o, y, ot):
        def term(p):
            return F.when(p > 0, p * F.log(p)).otherwise(F.lit(0.0))
        return -(term(o) + term(y) + term(ot))

    pivot = pivot.withColumn("total", F.col("oshi") + F.col("yotsu") + F.col("other")).withColumn("oshi_pct", F.when(F.col("total")>0, F.col("oshi")/F.col("total")).otherwise(F.lit(0.0))).withColumn("yotsu_pct", F.when(F.col("total")>0, F.col("yotsu")/F.col("total")).otherwise(F.lit(0.0))).withColumn("other_pct", F.when(F.col("total")>0, F.col("other")/F.col("total")).otherwise(F.lit(0.0))).withColumn("kimarite_entropy", entropy_expr(F.col("oshi_pct"), F.col("yotsu_pct"), F.col("other_pct")))

    pivot = pivot.withColumn(
        "specialist",
        F.when((F.col("oshi_pct") > F.col("yotsu_pct")) & (F.col("oshi_pct") > F.col("other_pct")), F.lit("oshi")).when((F.col("yotsu_pct") > F.col("oshi_pct")) & (F.col("yotsu_pct") > F.col("other_pct")), F.lit("yotsu")).otherwise(F.lit("other"))
    )

    rikishi_profile = pivot.select(F.col("winnerId").alias("rikishiId"), "specialist", "kimarite_entropy")

    # Join back to main to create specialist/kimarite_entropy columns per side
    main = (
        main
        .join(rikishi_profile.withColumnRenamed("specialist", "west_specialist").withColumnRenamed("kimarite_entropy","west_kimarite_entropy"), on=main.westId == F.col("rikishiId"), how="left").drop("rikishiId")
        .join(rikishi_profile.withColumnRenamed("specialist", "east_specialist").withColumnRenamed("kimarite_entropy","east_kimarite_entropy"), on=main.eastId == F.col("rikishiId"), how="left").drop("rikishiId")
        .withColumn("west_specialist_oshi", (F.col("west_specialist")=="oshi").cast("int"))
        .withColumn("west_specialist_yotsu", (F.col("west_specialist")=="yotsu").cast("int"))
        .withColumn("west_specialist_other", (F.col("west_specialist")=="other").cast("int"))
        .withColumn("east_specialist_oshi", (F.col("east_specialist")=="oshi").cast("int"))
        .withColumn("east_specialist_yotsu", (F.col("east_specialist")=="yotsu").cast("int"))
        .withColumn("east_specialist_other", (F.col("east_specialist")=="other").cast("int"))
    )

    # Winrates pivot per rikishi vs opponent specialist
    west_view = main.select(F.col("westId").alias("rikishiId"), F.col("east_specialist").alias("opponent_specialist"), F.when(F.col("westWin") == 1, 1).otherwise(0).alias("win"))
    east_view = main.select(F.col("eastId").alias("rikishiId"), F.col("west_specialist").alias("opponent_specialist"), F.when(F.col("westWin") == 0, 1).otherwise(0).alias("win"))
    rikishi_vs = west_view.union(east_view)

    winrates = rikishi_vs.groupBy("rikishiId", "opponent_specialist").agg(F.count("*").alias("bouts"), F.sum("win").alias("wins")).withColumn("winrate", F.when(F.col("bouts")>0, F.col("wins")/F.col("bouts")).otherwise(F.lit(None)))
    winrates_pivot = winrates.groupBy("rikishiId").pivot("opponent_specialist", ["oshi","yotsu","other"]).agg(F.first("winrate"))

    west_wr = winrates_pivot.withColumnRenamed("oshi","west_vs_oshi_winrate").withColumnRenamed("yotsu","west_vs_yotsu_winrate").withColumnRenamed("other","west_vs_other_winrate")
    east_wr = winrates_pivot.withColumnRenamed("oshi","east_vs_oshi_winrate").withColumnRenamed("yotsu","east_vs_yotsu_winrate").withColumnRenamed("other","east_vs_other_winrate")

    with_west = main.join(west_wr, main.westId == west_wr.rikishiId, "left").drop(west_wr.rikishiId)
    with_both = with_west.join(east_wr, with_west.eastId == east_wr.rikishiId, "left").drop(east_wr.rikishiId)

    main = (
        with_both
        .withColumn("west_winrate_vs_opponent_specialist", F.when(F.col("east_specialist") == "oshi", F.col("west_vs_oshi_winrate")).when(F.col("east_specialist") == "yotsu", F.col("west_vs_yotsu_winrate")).when(F.col("east_specialist") == "other", F.col("west_vs_other_winrate")).otherwise(F.lit(None)))
        .withColumn("east_winrate_vs_opponent_specialist", F.when(F.col("west_specialist") == "oshi", F.col("east_vs_oshi_winrate")).when(F.col("west_specialist") == "yotsu", F.col("east_vs_yotsu_winrate")).when(F.col("west_specialist") == "other", F.col("east_vs_other_winrate")).otherwise(F.lit(None)))
    )

    main = main.na.fill(0)

    # feature columns used in training (subset copied from training job)
    feature_cols = [
        "west_yusho", "west_makuuchiWins", "west_Makuuchi_basho",
        "east_yusho", "east_makuuchiWins", "east_Makuuchi_basho",
        "west_order", "east_order", "east_years_active", "west_years_active",
        "rolling_winloss_west", "rolling_winloss_east", "west_winRate", "east_winRate",
        "west_makuuchiWinRate", "east_makuuchiWinRate", "height_difference", "weight_difference",
        "west_kimarite_entropy", "east_kimarite_entropy", "west_specialist_oshi", "east_specialist_oshi",
        "west_specialist_yotsu", "east_specialist_yotsu", "west_specialist_other", "east_specialist_other",
        "west_winrate_vs_opponent_specialist", "east_winrate_vs_opponent_specialist"
    ]

    # For each rikishi, find the latest row where they appear (either side) and extract features mapped to west_/east_ prefixes
    def latest_features_for_rikishi(rikishi_id, target_prefix):
        if rikishi_id is None:
            return {}
        cond = (F.col("westId") == rikishi_id) | (F.col("eastId") == rikishi_id)
        q = main.filter(cond).orderBy(F.col("match_ts").desc())
        row = q.limit(1).collect()
        if not row:
            return {}
        row = row[0].asDict()
        out = {}
        # If the rikishi was on west side in that row, map west_* columns to target_prefix
        side = None
        if row.get("westId") == rikishi_id:
            side = "west"
        elif row.get("eastId") == rikishi_id:
            side = "east"

        for c in feature_cols:
            # c is already prefixed as west_ or east_
            if c in row:
                # compute target column name: replace leading west_/east_ with target_prefix
                if c.startswith("west_") or c.startswith("east_"):
                    feature_name = c.split("_", 1)[1]
                    out[f"{target_prefix}_{feature_name}"] = row[c]
                else:
                    # feature is not side-prefixed in list (rare) — assign to target as-is
                    out[f"{target_prefix}_{c}"] = row[c]

        return out

    west_features = latest_features_for_rikishi(westId, "west")
    east_features = latest_features_for_rikishi(eastId, "east")

    if not west_features and not east_features:
        logger.error("Could not extract features for either rikishi: %s %s", westId, eastId)
        raise LookupError(f"No feature snapshots found for rikishi ids: {westId}, {eastId}")
    if not west_features and east_features:
        logger.info("West features missing; voting for east (return 0)")
        return 0
    if not east_features and west_features:
        logger.info("East features missing; voting for west (return 1)")
        return 1

    features = {}
    features.update(west_features)
    features.update(east_features)

    # Keep only required final model columns (remove any extras)
    final_cols = []
    for c in feature_cols:
        if c.startswith("west_") or c.startswith("east_"):
            feature_name = c.split("_", 1)[1]
            final_cols.append(f"west_{feature_name}")
            final_cols.append(f"east_{feature_name}")
    # dedupe
    final_cols = list(dict.fromkeys(final_cols))

    # build final features dict keeping only final_cols
    final_features = {k: v for k, v in features.items() if k in final_cols}

    if not final_features:
        logger.error("No final features after filtering; features dict keys: %s", list(features.keys()))
        raise LookupError("No final model features could be assembled for prediction")

    input_df = spark_sess.createDataFrame([final_features])
    logger.debug("Input DataFrame columns for model: %s", input_df.columns)

    try:
        scored = mdl.transform(input_df)
        pred_row = scored.select("prediction").limit(1).collect()
        if not pred_row:
            logger.error("Model produced no prediction rows")
            raise RuntimeError("Model did not produce a prediction")
        pred = int(pred_row[0][0])
        logger.info("Model prediction for westId=%s eastId=%s -> %s", westId, eastId, pred)
        return pred
    except Exception as exc:
        logger.exception("Error running model.transform: %s", exc)
        raise RuntimeError(f"Model inference failed: {exc}")
