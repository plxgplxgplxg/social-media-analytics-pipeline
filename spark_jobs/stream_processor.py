import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo import UpdateOne
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_timestamp,
    from_json,
    length,
    lit,
    struct,
    to_json,
    to_timestamp,
    trim,
    window,
)

from config.elasticsearch_config import ELASTICSEARCH_HOST, POSTS_INDEX
from config.kafka_config import (
    AGGREGATED_METRICS_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    PROCESSED_POSTS_TOPIC,
    RAW_POSTS_TOPIC,
)
from config.minio_config import (
    AGGREGATES_BUCKET,
    CHECKPOINTS_BUCKET,
    CLEAN_POSTS_BUCKET,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)
from config.mongo_config import (
    MONGO_DATABASE,
    MONGO_URI,
    POSTS_COLLECTION,
    SENTIMENT_COLLECTION,
    TRENDING_COLLECTION,
)
from schemas.post_schema import POST_SPARK_SCHEMA
from spark_jobs.sentiment_udf import sentiment_udf
from spark_jobs.trending_analyzer import build_trending_keywords_df


SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)
SPARK_ES_PACKAGE = os.getenv(
    "SPARK_ES_PACKAGE",
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.0",
)
SPARK_AWS_PACKAGE = os.getenv(
    "SPARK_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.3.4"
)
CHECKPOINT_BASE = os.getenv(
    "CHECKPOINT_BASE", f"s3a://{CHECKPOINTS_BUCKET}/stream_processor"
)
ENABLE_MINIO_SINK = os.getenv("ENABLE_MINIO_SINK", "true").lower() == "true"
ENABLE_MONGO_SINK = os.getenv("ENABLE_MONGO_SINK", "true").lower() == "true"
ENABLE_ES_SINK = os.getenv("ENABLE_ES_SINK", "true").lower() == "true"
RESET_CHECKPOINT_ON_START = (
    os.getenv("RESET_CHECKPOINT_ON_START", "false").lower() == "true"
)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def normalize_mongo_records(
    records: list[dict], datetime_fields: tuple[str, ...]
) -> list[dict]:
    normalized_records: list[dict] = []
    for record in records:
        normalized = record.copy()
        for field_name in datetime_fields:
            normalized[field_name] = parse_iso_datetime(normalized.get(field_name))
        normalized_records.append(normalized)
    return normalized_records


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SocialMediaPipeline")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.jars.packages",
            ",".join(
                [SPARK_KAFKA_PACKAGE, SPARK_ES_PACKAGE, SPARK_AWS_PACKAGE]
            ),
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def reset_checkpoint_if_requested(spark: SparkSession) -> None:
    if not RESET_CHECKPOINT_ON_START:
        return

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    checkpoint_path = jvm.org.apache.hadoop.fs.Path(CHECKPOINT_BASE)
    filesystem = checkpoint_path.getFileSystem(hadoop_conf)

    if filesystem.exists(checkpoint_path):
        filesystem.delete(checkpoint_path, True)
        print(f"Deleted checkpoint state at {CHECKPOINT_BASE}")
    else:
        print(f"No checkpoint state found at {CHECKPOINT_BASE}")


def build_clean_stream(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", RAW_POSTS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), POST_SPARK_SCHEMA).alias("data")
    ).select("data.*")

    clean_df = (
        parsed_df.filter(col("title").isNotNull())
        .filter(length(trim(col("title"))) > 0)
        .withColumn("title", trim(col("title")))
        .withColumn("content", trim(coalesce(col("content"), col("title"))))
        .withColumn("source", coalesce(col("source"), lit("unknown")))
        .withColumn("event_time", to_timestamp(col("published_at")))
        .withColumn("event_time", coalesce(col("event_time"), current_timestamp()))
        .withColumn("published_at", coalesce(col("published_at"), col("ingested_at")))
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["id"])
    )

    return clean_df.withColumn("sentiment", sentiment_udf(col("title")))


def build_processed_sink_df(enriched_df: DataFrame) -> DataFrame:
    return enriched_df.select(
        col("id").cast("string").alias("key"),
        to_json(
            struct(
                "id",
                "source",
                "title",
                "content",
                "url",
                "author",
                "published_at",
                "subreddit",
                "feed_name",
                "ingested_at",
                "event_time",
                "sentiment",
            )
        ).alias("value"),
    )


def build_sentiment_metrics_df(enriched_df: DataFrame) -> DataFrame:
    return (
        enriched_df.groupBy(window(col("event_time"), "15 minutes"), col("sentiment"))
        .agg(count("*").alias("post_count"))
        .select(
            lit("sentiment").alias("metric_type"),
            col("sentiment"),
            col("post_count"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
        )
    )


def build_aggregated_metrics_sink_df(
    sentiment_metrics_df: DataFrame, trending_df: DataFrame
) -> DataFrame:
    sentiment_json_df = sentiment_metrics_df.select(
        lit("sentiment").alias("key"),
        to_json(
            struct(
                "metric_type",
                "sentiment",
                "post_count",
                "window_start",
                "window_end",
            )
        ).alias("value"),
    )
    trending_json_df = trending_df.select(
        lit("trending").alias("key"),
        to_json(
            struct(
                lit("trending").alias("metric_type"),
                col("keyword"),
                col("frequency"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
            )
        ).alias("value"),
    )
    return sentiment_json_df.unionByName(trending_json_df)


def write_dataframe_to_parquet(batch_df: DataFrame, target_path: str) -> None:
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write.mode("append")
        .format("parquet")
        .save(target_path)
    )


def write_aggregated_metrics_to_kafka(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", AGGREGATED_METRICS_TOPIC)
        .save()
    )


def write_posts_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    records = [json.loads(row) for row in batch_df.toJSON().collect()]
    if not records:
        return
    records = normalize_mongo_records(
        records, ("published_at", "ingested_at", "event_time")
    )
    client = MongoClient(MONGO_URI)
    try:
        client[MONGO_DATABASE][POSTS_COLLECTION].insert_many(records, ordered=False)
    finally:
        client.close()


def write_sentiment_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    records = [json.loads(row) for row in batch_df.toJSON().collect()]
    if not records:
        return
    records = normalize_mongo_records(records, ("window_start", "window_end"))
    client = MongoClient(MONGO_URI)
    try:
        client[MONGO_DATABASE][SENTIMENT_COLLECTION].insert_many(
            records, ordered=False
        )
    finally:
        client.close()


def write_trending_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    records = [json.loads(row) for row in batch_df.toJSON().collect()]
    if not records:
        return
    records = normalize_mongo_records(records, ("window_start", "window_end"))
    client = MongoClient(MONGO_URI)
    try:
        operations = [
            UpdateOne(
                {
                    "window_start": record["window_start"],
                    "window_end": record["window_end"],
                    "keyword": record["keyword"],
                },
                {"$set": record},
                upsert=True,
            )
            for record in records
        ]
        if operations:
            client[MONGO_DATABASE][TRENDING_COLLECTION].bulk_write(
                operations, ordered=False
            )
    finally:
        client.close()


def write_posts_to_es(batch_df: DataFrame, batch_id: int) -> None:
    records = [json.loads(row) for row in batch_df.toJSON().collect()]
    if not records:
        return
    client = Elasticsearch(ELASTICSEARCH_HOST)
    try:
        for record in records:
            client.index(index=POSTS_INDEX, id=record["id"], document=record)
    finally:
        client.close()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    reset_checkpoint_if_requested(spark)

    enriched_df = build_clean_stream(spark)
    trending_df = build_trending_keywords_df(enriched_df)
    sentiment_metrics_df = build_sentiment_metrics_df(enriched_df)

    queries = []

    processed_query = (
        build_processed_sink_df(enriched_df)
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", PROCESSED_POSTS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/processed_posts_kafka")
        .outputMode("append")
        .start()
    )
    queries.append(processed_query)

    aggregated_query = (
        build_aggregated_metrics_sink_df(sentiment_metrics_df, trending_df)
        .writeStream.foreachBatch(write_aggregated_metrics_to_kafka)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/aggregated_metrics_kafka")
        .outputMode("update")
        .start()
    )
    queries.append(aggregated_query)

    if ENABLE_MINIO_SINK:
        queries.append(
            enriched_df.writeStream.format("parquet")
            .option("path", f"s3a://{CLEAN_POSTS_BUCKET}/posts")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/clean_posts_parquet")
            .outputMode("append")
            .start()
        )
        queries.append(
            trending_df.writeStream.foreachBatch(
                lambda batch_df, batch_id: write_dataframe_to_parquet(
                    batch_df, f"s3a://{AGGREGATES_BUCKET}/trending_topics"
                )
            )
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/trending_parquet")
            .outputMode("update")
            .start()
        )
        queries.append(
            sentiment_metrics_df.writeStream.foreachBatch(
                lambda batch_df, batch_id: write_dataframe_to_parquet(
                    batch_df, f"s3a://{AGGREGATES_BUCKET}/sentiment_metrics"
                )
            )
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/sentiment_parquet")
            .outputMode("update")
            .start()
        )

    if ENABLE_MONGO_SINK:
        queries.append(
            enriched_df.writeStream.foreachBatch(write_posts_to_mongo)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/mongo_posts")
            .outputMode("append")
            .start()
        )
        queries.append(
            sentiment_metrics_df.writeStream.foreachBatch(write_sentiment_to_mongo)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/mongo_sentiment")
            .outputMode("update")
            .start()
        )
        queries.append(
            trending_df.writeStream.foreachBatch(write_trending_to_mongo)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/mongo_trending")
            .outputMode("update")
            .start()
        )

    if ENABLE_ES_SINK:
        queries.append(
            enriched_df.writeStream.foreachBatch(write_posts_to_es)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/es_posts")
            .outputMode("append")
            .start()
        )

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
