"""Show the 20 most frequent tokens in the supplied Q4 text file."""

import argparse

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", help="Text file visible to the Spark cluster")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Q4 WordCount - Hoin Lee").getOrCreate()
    try:
        lines = spark.read.text(args.input_path)
        words = lines.select(F.explode(F.split("value", r"\s+")).alias("word"))
        frequencies = words.where(F.col("word") != "").groupBy("word").count()
        frequencies.orderBy(F.desc("count"), F.asc("word")).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
