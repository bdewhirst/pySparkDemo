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
    data = data.withColumn("from_date", pyspark.sql.functions.lit("12/01/10 08:26"))
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

    recency = recency.join(
        recency.groupBy("CustomerID").agg(
            pyspark.sql.functions.max("recency").alias("recency")
        ),
        on="recency",
        how="leftsemi",
    )
    # recency.show(n=5, truncate=0)
    # recency.printSchema()
    return recency


def freq_analysis(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    freq_peek = data.groupby("CustomerID").agg(
        pyspark.sql.functions.count("InvoiceDate").alias("frequency")
    )
    # freq_peek.show(5, 0)

    freq = data.join(freq_peek, on="CustomerID", how="inner")
    # freq.printSchema()
    return freq


def monetary_analysis(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    m_val = data.withColumn(
        "TotalAmount",
        pyspark.sql.functions.col("Quantity") * pyspark.sql.functions.col("UnitPrice"),
    )
    m_val = m_val.groupBy("CustomerID").agg(
        pyspark.sql.functions.sum("TotalAmount").alias("monetary_value")
    )
    return m_val


def finalize_rfm(
    spark: pyspark.sql.SparkSession,
    data0: pyspark.sql.DataFrame,
    data1: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    final_data = data0.join(data1, on="CustomerID", how="inner")
    final_data = final_data.select(
        ["recency", "frequency", "monetary_value", "CustomerID"]
    ).distinct()
    # final_data.show(5, 0)
    return final_data


def standardize(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    # scale these features and return the result
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.feature import StandardScaler

    assemble = VectorAssembler(
        inputCols=["recency", "frequency", "monetary_value"], outputCol="features"
    )

    assembled_data = assemble.transform(data)

    scale = StandardScaler(inputCol="features", outputCol="standardized")
    data_scale = scale.fit(assembled_data)
    data_scale_output = data_scale.transform(assembled_data)
    # data_scale_output.select("standardized").show(2, truncate=False)
    return data_scale_output


def build_mach_learn_model(
    spark: pyspark.sql.SparkSession, data: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    # this part, I'm not planning on refactoring-- it switches over to pandas/workflows I'm not focusing on here
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    import numpy as np

    cost = np.zeros(10)

    evaluator = ClusteringEvaluator(
        predictionCol="prediction",
        featuresCol="standardized",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    for i in range(2, 10):
        KMeans_algo = KMeans(featuresCol="standardized", k=i)
        KMeans_fit = KMeans_algo.fit(data)
        output = KMeans_fit.transform(data)
        cost[i] = KMeans_fit.summary.trainingCost

    import pandas as pd
    import matplotlib.pyplot as plt
    df_cost = pd.DataFrame(cost[2:])
    df_cost.columns = ["cost"]
    new_col = range(2, 10)
    df_cost.insert(0, 'cluster', new_col)
    plt.plot(df_cost.cluster, df_cost.cost)
    plt.xlabel('Number of Clusters')
    plt.ylabel('Score')
    plt.title('Elbow Curve')
    plt.show()


def main() -> None:
    # see readme.md; this is (initially) from a tutorial.
    print("start PySpark refresher")
    spark = get_spark()
    ecomm_data = spark.read.csv("data/datacamp_ecommerce.csv", header=True, escape='"')

    # overview of exploratory analysis with pyspark:
    # early_analysis(spark=spark, data=ecomm_data)
    # next_analysis(spark=spark, data=ecomm_data)

    # prep for machine learning with RFM-- recency, frequency, monetary aggs:
    recency = recency_analysis(spark=spark, data=ecomm_data)
    frequency = freq_analysis(spark=spark, data=recency)
    monetary = monetary_analysis(spark=spark, data=frequency)
    final_rfm_data = finalize_rfm(spark=spark, data0=monetary, data1=frequency)

    standardized = standardize(spark=spark, data=final_rfm_data)

    build_mach_learn_model(spark=spark, data=standardized)

if __name__ == "__main__":
    main()
