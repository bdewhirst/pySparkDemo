# Brian Dewhirst, 2023-11-09, b.dewhirst@gmail.com

import pyspark  # left explicit for clarity


def get_spark() -> pyspark.sql.SparkSession:
    # ref on SparkSession: https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html
    spark = (
        pyspark.sql.SparkSession.builder.appName("Datacamp Pyspark Tutorial")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "10g")
        .getOrCreate()
    )
    return spark


def early_analysis(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> None:
    # adapted from the datacamps tutorial
    data.show(
        n=5, truncate=0
    )  # recall: same idea as head, but works with pyspark dataframes. has own print/printf methods
    print("\r\n")
    print(data.count())
    print("\r\n")
    print(data.select("CustomerID").distinct().count())

    # find country with most purchases
    data.groupBy("Country").agg(
        pyspark.sql.functions.countDistinct("CustomerID").alias("country_count")
    ).show()

    # as before, but sorted:
    data.groupBy("Country").agg(
        pyspark.sql.functions.countDistinct("CustomerID").alias("country_count")
    ).orderBy(pyspark.sql.functions.desc("country_count")).show()

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    data = data.withColumn(
        "date", pyspark.sql.functions.to_timestamp("InvoiceDate", "yy/MM/dd HH:mm")
    )


def next_analysis(spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame) -> None:
    # when was the most recent purch.
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    data = data.withColumn(
        "date", pyspark.sql.functions.to_timestamp("InvoiceDate", "yy/MM/dd HH:mm")
    )
    # debugging statements:
    """
    data.show(5,0)
    print(data.columns)
    print(data.select("date").distinct().count())
    """

    # tutorial code:  # data.select(max("date")).show()  # hypothesis: this isn't the correct 'max' function

    # fix (which is why it is helpful to be explicit with which functions we're calling.):
    data.select(pyspark.sql.functions.max("date")).show()


def main() -> None:
    # see readme.md; this is (initially) from a tutorial.
    print("start PySpark refresher")
    spark = get_spark()
    ecomm_data = spark.read.csv("data/datacamp_ecommerce.csv", header=True, escape='"')
    # early_analysis(spark=spark, data=ecomm_data)
    next_analysis(spark=spark, data=ecomm_data)


if __name__ == "__main__":
    main()
