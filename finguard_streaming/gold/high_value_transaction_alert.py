from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

#This shows a join of a stream table 'transaction' with a static table (materialized view) 'customers'
@dp.table(name="finguard.gold.transaction", comment="High value transaction alert")
def high_value_transaction_alert() -> DataFrame:

    transaction = spark.readStream.table("finguard.silver.transaction")
    customers = spark.read.table("finguard.silver.customers")

    joined_df = (
        transaction.alias("t")
        .join(
            customers.alias("c"),
            on=col("t.customer_id") == col("c.customer_id"),
            how="left",
        )
        .filter(col("t.amount") > col("c.transaction_limit"))
        .select(
            # some extra columns for gold layer
            concat_ws("-", lit("ALERT"), col("t.transaction_id")).alias("alert_id"),
            lit("High Value Transaction").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),
            # values from silver layer joined of both transaction and customers
            col("t.transaction_id"),
            col("t.customer_id"),
            col("c.email").alias("customer_email"),
            concat_ws(" ", col("c.first_name"), col("c.last_name")).alias("customer_name"),
            col("t.amount").alias("transaction_amount"),
            col("c.transaction_limit"),
            col("t.currency"),
            col("t.merchant_name"),
            col("t.merchant_category"),
            col("t.transaction_type"),
            col("t.payment_channel"),
            col("t.city"),
            col("t.country"),
            col("t.is_international"),
            col("t.transaction_timestamp"),
            col("t.status")
        )
    )

    return joined_df
