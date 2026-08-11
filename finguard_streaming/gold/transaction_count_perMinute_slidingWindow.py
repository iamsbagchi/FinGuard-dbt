from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


# This show how to use Tumbling window to count the number of transactions per minute.
# The transaction_timestamp is used as the watermark to ensure that we have all the data for the last 5 minutes.


@dp.table(
    name="finguard.gold.transaction_count_per_minute_sliding", comment="Cleaned Fraud watchlist"
)
def transaction_count_per_minute() -> DataFrame:

    transaction_df = spark.readStream.table("finguard.silver.transaction")

    transaction_df = transaction_df.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df = (
        transaction_df.groupBy(window("transaction_timestamp", "5 minute", "1 minute"))
        .agg(count("*").alias("transaction_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "transaction_count",
        )
    )

    return transaction_count_df
