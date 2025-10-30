"""
first retrieve the model from s3, load it into memory, then prepare the data as needed to pass into the model for prediction

will need to, get the rikishi ids from the webhook, then find the rows from the pyspark dataframe to construct a row built with the two (westId and eastId)

if the rikishi id doesn't exist then vote for the other. if neither exist 50-50 it

return the predictions back to the sparkNewMatches job to then be stored as a key value in mongo

"""

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = (
    SparkSession.builder
        .appName("inference_job")
        # same S3A config you used for training
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
)

model_path = "s3a://ryans-sumo-bucket/models/xgboost_model"
inference_model = PipelineModel.load(model_path)

rows = [
    {
        "bashoId": "202501",
        "eastRikishi": "terunofuji",
        "westRikishi": "kotonowaka",
        "day": 3,
        "venue": "Tokyo",
    },
    {
        "bashoId": "202501",
        "eastRikishi": "houshouryu",
        "westRikishi": "daieishou",
        "day": 3,
        "venue": "Tokyo",
    },
]

predict_df = spark.createDataFrame(rows)
scored = inference_model.transform(predict_df)
scored.select(
    "bashoId",
    "eastRikishi",
    "westRikishi",
    "prediction",
    "probability",
).show(truncate=False)
