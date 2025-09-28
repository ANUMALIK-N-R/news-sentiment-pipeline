from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml import PipelineModel
import os

MODEL_PATH = "models/news_sentiment_pipeline"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_URL", "kafka:9092")

INPUT_TOPIC = "news-raw"
OUTPUT_TOPIC = "news-predictions"

spark = SparkSession.builder \
    .appName("RealTimeNewsSentiment") \
    .master("local[*]") \
    .getOrCreate()

pipeline_model = PipelineModel.load(MODEL_PATH)

schema = StructType([StructField("title", StringType(), True)])

raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", INPUT_TOPIC) \
    .load()

json_df = raw.selectExpr("CAST(value AS STRING) as json_str")
parsed = json_df.select(from_json(col("json_str"), schema).alias("data"))
titles = parsed.select("data.title").na.drop()

preds = pipeline_model.transform(titles)

label_map = {0.0: "Negative", 1.0: "Neutral", 2.0: "Positive"}
decode_udf = udf(lambda v: label_map.get(float(v), "Unknown"), StringType())

output = preds.select(
    to_json(struct(col("title"), decode_udf(
        col("prediction")).alias("sentiment"))).alias("value")
)

query = output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/news_checkpoint") \
    .start()

query.awaitTermination()
