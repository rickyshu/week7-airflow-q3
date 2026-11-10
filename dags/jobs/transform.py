"""Count Netflix titles by type and genre and write Snappy Parquet."""

import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-year", default=2015, type=int)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Q9 Netflix genre counts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        titles = spark.read.options(
            header=True, multiLine=True, escape='"', mode="FAILFAST"
        ).csv(args.input)
        genres = titles.where(F.col("release_year").cast("int") >= args.min_year).select(
            "type", F.explode(F.split("listed_in", ",")).alias("genre")
        ).withColumn("genre", F.trim("genre"))
        counts = genres.where(F.col("genre") != "").groupBy("type", "genre").agg(
            F.count("*").alias("title_count")
        )
        counts.coalesce(1).write.mode("overwrite").parquet(args.output, compression="snappy")
        row_count = spark.read.parquet(args.output).count()
        print(f"Minimum release year: {args.min_year}", flush=True)
        print(f"Aggregated rows: {row_count}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
