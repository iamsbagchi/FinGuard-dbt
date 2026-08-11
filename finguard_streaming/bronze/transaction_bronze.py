from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    name = 'finguard.bronze.transaction',
    comment = 'Transaction raw stream data ingested by Kafka topic'
)
def transaction_bronze() -> DataFrame:
   bootstrap_servers = dbutils.secrets.get(scope = "finguard-scope", key = "bootstrap_servers")
   api_key = dbutils.secrets.get(scope = "finguard-scope", key = "api_key")
   api_secret = dbutils.secrets.get(scope = "finguard-scope", key = "api_secret")
   topic = dbutils.secrets.get(scope = "finguard-scope", key = "topic")
   jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'
   streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("startingOffsets", "earliest")
    # .option("endingOffsets", "latest")
    .load()
    )
   parsed_streaming_df = streaming_df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType"),
    current_timestamp().alias("ingest_timestamp")
    )
   
   return parsed_streaming_df
   







