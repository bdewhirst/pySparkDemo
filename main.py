# Brian Dewhirst, 2023-11-09, b.dewhirst@gmail.com

import pyspark


def main() -> None:
    # see readme.md; this is (initially) from a tutorial.
    print("start refresher")
    spark = (
        pyspark.sql.SparkSession.builder.appName("Datacamp Pyspark Tutorial")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "10g")
        .getOrCreate()
    )
    df = spark.read.csv("data/datacamp_ecommerce.csv", header=True, escape='"')


if __name__ == "__main__":
    main()
