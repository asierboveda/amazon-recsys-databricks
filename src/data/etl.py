from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.config import load_paths_config
from src.utils.spark_session import create_spark_session

LOGGER = logging.getLogger("etl")


REVIEWS_SCHEMA = StructType(
    [
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("overall", DoubleType(), True),
        StructField("unixReviewTime", LongType(), True),
        StructField("reviewTime", StringType(), True),
        StructField("verified", StringType(), True),
        StructField("vote", StringType(), True),
        StructField("helpful", ArrayType(LongType()), True),
    ]
)

METADATA_SCHEMA = StructType(
    [
        StructField("asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("category", ArrayType(StringType()), True),
        StructField("categories", ArrayType(ArrayType(StringType())), True),
        StructField("also_buy", ArrayType(StringType()), True),
        StructField("also_view", ArrayType(StringType()), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Amazon Product Data ETL")
    parser.add_argument("--spark-config", default="conf/spark_config.json")
    parser.add_argument("--paths-config", default="conf/paths.json")
    parser.add_argument("--reviews-input", default=None)
    parser.add_argument("--metadata-input", default=None)
    parser.add_argument("--interactions-output", default=None)
    parser.add_argument("--catalog-output", default=None)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    return parser.parse_args()


def _target_partitions(spark: SparkSession) -> int:
    return int(spark.conf.get("spark.sql.shuffle.partitions", "200"))


def _read_reviews(spark: SparkSession, input_glob: str) -> DataFrame:
    return (
        spark.read.format("json")
        .schema(REVIEWS_SCHEMA)
        .option("multiLine", "false")
        .load(input_glob)
    )


def _read_metadata(spark: SparkSession, input_glob: str) -> DataFrame:
    return (
        spark.read.format("json")
        .schema(METADATA_SCHEMA)
        .option("multiLine", "false")
        .load(input_glob)
    )


def transform_interactions(reviews_df: DataFrame) -> DataFrame:
    event_ts = F.coalesce(
        F.to_timestamp(F.from_unixtime(F.col("unixReviewTime"))),
        F.to_timestamp(F.col("reviewTime"), "MM dd, yyyy"),
    )

    reviews_enriched = (
        reviews_df.select(
            F.col("reviewerID").alias("user_id"),
            F.col("asin").alias("item_id"),
            F.col("overall").cast("double").alias("rating"),
            event_ts.alias("event_ts"),
            F.coalesce(F.col("verified").cast("boolean"), F.lit(False)).alias("verified_purchase"),
            F.regexp_replace(F.coalesce(F.col("vote"), F.lit("0")), ",", "")
            .cast("int")
            .alias("vote_count"),
            F.element_at(F.col("helpful"), 1).cast("int").alias("helpful_yes"),
            F.element_at(F.col("helpful"), 2).cast("int").alias("helpful_total"),
        )
        .where(F.col("user_id").isNotNull() & F.col("item_id").isNotNull() & F.col("rating").isNotNull())
        .withColumn("event_ts", F.coalesce(F.col("event_ts"), F.current_timestamp()))
    )

    interactions_df = (
        reviews_enriched.groupBy("user_id", "item_id")
        .agg(
            F.avg("rating").alias("rating"),
            F.max("event_ts").alias("last_event_ts"),
            F.max("verified_purchase").alias("verified_purchase"),
            F.sum(F.coalesce(F.col("vote_count"), F.lit(0))).alias("vote_count"),
            F.sum(F.coalesce(F.col("helpful_yes"), F.lit(0))).alias("helpful_yes"),
            F.sum(F.coalesce(F.col("helpful_total"), F.lit(0))).alias("helpful_total"),
            F.count(F.lit(1)).alias("interaction_count"),
        )
        .withColumn("event_year", F.year("last_event_ts").cast("int"))
        .withColumn("event_month", F.month("last_event_ts").cast("int"))
    )

    return interactions_df


def transform_catalog(metadata_df: DataFrame) -> DataFrame:
    first_nested_path = F.element_at(F.col("categories"), 1)
    empty_string_array = F.array().cast("array<string>")
    category_path = F.coalesce(
        F.concat_ws(" > ", F.col("category")),
        F.concat_ws(" > ", first_nested_path),
    )
    category_l1 = F.coalesce(
        F.element_at(F.col("category"), 1),
        F.element_at(first_nested_path, 1),
    )

    catalog_df = (
        metadata_df.select(
            F.col("asin").alias("item_id"),
            F.trim(F.col("title")).alias("title"),
            F.trim(F.col("brand")).alias("brand"),
            F.regexp_extract(F.coalesce(F.col("price"), F.lit("")), r"([0-9]+(?:\.[0-9]+)?)", 1)
            .cast("double")
            .alias("price"),
            category_path.alias("category_path"),
            category_l1.alias("category_l1"),
            F.size(F.coalesce(F.col("also_buy"), empty_string_array)).alias("also_buy_count"),
            F.size(F.coalesce(F.col("also_view"), empty_string_array)).alias("also_view_count"),
        )
        .where(F.col("item_id").isNotNull())
        .dropDuplicates(["item_id"])
        .withColumn("category_l1", F.coalesce(F.col("category_l1"), F.lit("unknown")))
    )
    return catalog_df


def write_parquet(
    df: DataFrame,
    output_path: str,
    mode: str,
    partition_cols: list[str],
    target_partitions: int,
) -> None:
    writer_df = df.repartition(target_partitions, *partition_cols)
    (
        writer_df.write.mode(mode)
        .option("compression", "snappy")
        .partitionBy(*partition_cols)
        .parquet(output_path)
    )


def run_etl(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    spark = create_spark_session(
        app_name="amazon-etl",
        spark_config_path=args.spark_config,
        paths_config_path=args.paths_config,
    )

    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        paths = load_paths_config(args.paths_config)

        reviews_input = args.reviews_input or paths["reviews_input_glob"]
        metadata_input = args.metadata_input or paths["metadata_input_glob"]
        interactions_output = args.interactions_output or paths["interactions_output"]
        catalog_output = args.catalog_output or paths["catalog_output"]
        n_partitions = _target_partitions(spark)

        LOGGER.info("Reading reviews from %s", reviews_input)
        reviews_df = _read_reviews(spark, reviews_input)
        interactions_df = transform_interactions(reviews_df)

        LOGGER.info("Writing interactions to %s", interactions_output)
        write_parquet(
            df=interactions_df,
            output_path=interactions_output,
            mode=args.mode,
            partition_cols=["event_year", "event_month"],
            target_partitions=n_partitions,
        )

        LOGGER.info("Reading metadata from %s", metadata_input)
        metadata_df = _read_metadata(spark, metadata_input)
        catalog_df = transform_catalog(metadata_df).withColumn(
            "category_partition",
            F.regexp_replace(F.lower(F.col("category_l1")), r"[^a-z0-9]+", "_"),
        )

        LOGGER.info("Writing catalog to %s", catalog_output)
        write_parquet(
            df=catalog_df,
            output_path=catalog_output,
            mode=args.mode,
            partition_cols=["category_partition"],
            target_partitions=n_partitions,
        )

        LOGGER.info("ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_etl(parse_args())
