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


def add_date(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    data = data.withColumn(
        "date", pyspark.sql.functions.to_timestamp("InvoiceDate", "yy/MM/dd HH:mm")
    )
    return data


def next_analysis(spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame) -> None:
    # when was the most recent purch.
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    data = add_date(spark=spark, data=data)
    # data = data.withColumn(
    #     "date", pyspark.sql.functions.to_timestamp("InvoiceDate", "yy/MM/dd HH:mm")
    # )
    # debugging statements:
    """
    data.show(5,0)
    print(data.columns)
    print(data.select("date").distinct().count())
    """

    # tutorial code:  # data.select(max("date")).show()  # hypothesis: this isn't the correct 'max' function

    # fix (which is why it is helpful to be explicit with which functions we're calling.):
    # ref: https://sparkbyexamples.com/pyspark/pyspark-max-different-methods-explained/
    data.select(pyspark.sql.functions.max("date")).show()


def recency_analysis(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    data = add_date(spark=spark, data=data)  # needed for subsequent steps
    # "RFM" = recency, frequency, monetary value
    # assign recency score to each customer

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")  # see if this fixes issue
    data = data.withColumn(
        "from_date", pyspark.sql.functions.lit("12/01/10 08:26")
    )
    data = data.withColumn(
        "from_date", pyspark.sql.functions.to_timestamp("from_date", "yy/MM/dd HH:mm")
    )
    recency = data.withColumn(
        "from_date",
        pyspark.sql.functions.to_timestamp(pyspark.sql.functions.col("from_date")),
    ).withColumn(
        "recency",
        pyspark.sql.functions.col("date").cast("long")
        - pyspark.sql.functions.col("from_date").cast("long"),
    )

    recency = recency.join(recency.groupBy('CustomerID').agg(pyspark.sql.functions.max('recency').alias('recency')), on='recency', how='leftsemi')
    recency.show(n=5, truncate=0)
    # aside:
    recency.printSchema()
    return recency


def freq_analysis(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> None:
    freq = data.groupby("CustomerID").agg(pyspark.sql.functions.count('InvoiceDate').alias('frequency'))
    freq.show(5,0)

def main() -> None:
    # see readme.md; this is (initially) from a tutorial.
    print("start PySpark refresher")
    spark = get_spark()
    ecomm_data = spark.read.csv("data/datacamp_ecommerce.csv", header=True, escape='"')

    # overview of exploratory analysis with pyspark:
    # early_analysis(spark=spark, data=ecomm_data)
    # next_analysis(spark=spark, data=ecomm_data)

    # ecomm_data = add_date(spark=spark, data=ecomm_data)  # needed for subsequent steps

    # prep for machine learning:
    recency = recency_analysis(spark=spark, data=ecomm_data)
    freq_analysis(spark=spark, data=recency)




if __name__ == "__main__":
    main()
