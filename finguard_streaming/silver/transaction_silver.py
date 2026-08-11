from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

expectationsOrDrop = {
    'valid_transaction_id': 'transaction_id IS NOT NULL',
    'valid_customer_id': 'customer_id IS NOT NULL',
    'valid_card_number': 'card_number IS NOT NULL',
    'valid_merchant_id': 'merchant_id IS NOT NULL'
    }

@dp.table(
    name = 'finguard.silver.transaction',
    comment = 'Transaction details flatten data from json value'
)
@dp.expect_all_or_drop(expectationsOrDrop)
@dp.expect('valid_amount', 'amount > 0')
def transaction_silver() -> DataFrame:
    bronze_df = spark.readStream.table('finguard.bronze.transaction')

    schema = """
        transaction_id STRING,
        customer_id STRING,
        card_number STRING,
        merchant_id STRING,
        merchant_name STRING,
        merchant_category STRING,
        amount DOUBLE,
        currency STRING,
        transaction_type STRING,
        payment_channel STRING,
        device_id STRING,
        city STRING,
        country STRING,
        transaction_timestamp TIMESTAMP,
        is_international BOOLEAN,
        status STRING
    """

    transformed_df = bronze_df.select(
        from_json(bronze_df.value.cast('string'), schema=schema).alias('data'),
        col('topic').alias('kafka_topic'),
        col('partition').alias('kafka_partition'),
        col('offset').alias('kafka_offset'),
        col('timestamp').alias('kafka_timestamp'),
        col('ingest_timestamp').alias('bronze_insgestion_timestamp')
    ).select(
        col('data.*'),
        col('kafka_topic'),
        col('kafka_partition'),
        col('kafka_offset'),
        col('kafka_timestamp'),
        col('bronze_insgestion_timestamp'),
        current_timestamp().alias('silver_ingestion_timestamp')
    )

    return transformed_df