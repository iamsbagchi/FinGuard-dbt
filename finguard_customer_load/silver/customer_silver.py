from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(
    name = 'finguard.silver.customers',
    comment = 'Parsed and cleaned customers data'
)
@dp.expect_or_drop('valid_cuatomer','customer_id IS NOT NULL')
def customer_silver() -> DataFrame:
    bronze_df = spark.read.table('finguard.bronze.customers')
    
    transformed_df = bronze_df.select(
    col('customer_id'),
    col('first_name'),
    col('last_name'),
    col('gender'),
    col('age'),
    col('city'),
    col('state'),
    col('country'),
    col('annual_income'),
    col('customer_segment'),
    to_date(col('account_open_date'),'yyyy-MM-dd').alias('account_open_date'),
    col('risk_score'),
    col('preferred_spending_min'),
    col('preferred_spending_max'),
    col('preferred_city'),
    col('preferred_country'),
    col('trusted_device_id'),
    col('card_number'),
    col('card_type'),
    col('email'),
    col('transaction_limit'),
    col('update_timestamp'),
    current_timestamp().alias('silver_ingestion_timestamp')
    )
    
    return transformed_df