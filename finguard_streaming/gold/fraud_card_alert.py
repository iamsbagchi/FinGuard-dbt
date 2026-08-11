from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


# This shows a join of a stream table 'transaction' with another stream table 'fraud_watchlist'
# and a static table (materialized view) 'customers'
@dp.table(name="finguard.gold.fraud_card_alert", comment="Cleaned Fraud watchlist")
def fraud_watchlist_gold() -> DataFrame:

    transaction_df = spark.readStream.table("finguard.silver.transaction")
    fraud_df = spark.readStream.table("finguard.silver.fraud_watchlist")
    customer = spark.read.table("finguard.silver.customers")

    transaction_df = transaction_df.withWatermark("transaction_timestamp", "5 minutes")
    fraud_df = fraud_df.withWatermark("effective_from", "5 minutes")

    fraud_detected = (
        transaction_df.alias("t")
        .join(
            fraud_df.alias("f"),
            on=col("t.card_number") == col("f.entity_id"),
            how="inner",
        )
        .join(
            customer.alias("c"),
            on=col("t.customer_id") == col("c.customer_id"),
            how="left",
        )
        .select(
            # Alert identification
            concat_ws(
                "-", lit("FRAUD"), col("transaction_id"), col("watchlist_id")
            ).alias("alert_id"),
            lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),
            # Transaction details
            transaction_df.transaction_id,
            transaction_df.customer_id,
            customer.email.alias("customer_email"),
            concat_ws(" ", customer.first_name, customer.last_name).alias(
                "customer_name"
            ),
            transaction_df.card_number,
            transaction_df.amount,
            transaction_df.currency,
            transaction_df.merchant_id,
            transaction_df.merchant_name,
            transaction_df.merchant_category,
            transaction_df.transaction_type,
            transaction_df.payment_channel,
            transaction_df.device_id,
            transaction_df.city.alias("transaction_city"),
            transaction_df.country.alias("transaction_country"),
            transaction_df.transaction_timestamp,
            transaction_df.is_international,
            transaction_df.status.alias("transaction_status"),
            # Fraud watchlist details
            col("watchlist_id"),
            col("watch_type"),
            col("risk_level"),
            col("action"),
            col("reason_code"),
            col("reason_description"),
            col("effective_from").alias("watchlist_effective_from"),
            col("reported_by"),
            col("reported_source"),
            fraud_df.city.alias("watchlist_city"),
            fraud_df.country.alias("watchlist_country"),
        )
    )
    return fraud_detected
