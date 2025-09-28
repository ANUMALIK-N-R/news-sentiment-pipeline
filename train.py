import argparse
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def load_bootstrap_data(spark, path):
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("label", DoubleType(), True),
    ])
    return spark.read.json(path, schema=schema, multiLine=True)


def train_and_save(data_path, out_path):
    spark = SparkSession.builder \
        .appName("NewsSentimentTraining") \
        .master("local[*]") \
        .getOrCreate()

    print(f"📂 Loading training data from {data_path}...")
    df = load_bootstrap_data(spark, data_path)
    df.show(5, truncate=False)

    # Pipeline: Tokenizer -> HashingTF -> IDF -> LogisticRegression
    tokenizer = Tokenizer(inputCol="title", outputCol="words")
    hashingTF = HashingTF(
        inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(maxIter=20, regParam=0.01, labelCol="label")

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

    # Train
    print("Training model...")
    model = pipeline.fit(df)

    # Evaluate training accuracy
    preds = model.transform(df)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    acc = evaluator.evaluate(preds)
    print(f"Training Accuracy: {acc:.2%}")

    # Save model
    print(f"Saving model to {out_path}...")
    model.write().overwrite().save(out_path)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train News Sentiment Classifier")
    parser.add_argument("--data", type=str, required=True,
                        help="Path to bootstrap JSON dataset")
    parser.add_argument(
        "--out", type=str, default="./models/news_sentiment_pipeline", help="Output model path")
    args = parser.parse_args()

    train_and_save(args.data, args.out)
