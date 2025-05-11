import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import bround, col, count, sum, desc, when
from pyspark.sql.window import Window
from collections import deque

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "spark-access-459420-f8b1f097831d.json"

# Start Spark session with optimization and parallelism settings
spark = SparkSession.builder \
    .appName("EthereumThiefAnalysis") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.ui.port", "4041") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=ERROR") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.logLevel=ERROR") \
    .config("spark.jars", "spark-bigquery-with-dependencies_2.12-0.32.0.jar") \
    .getOrCreate()

print("Spark session initialized successfully.")
print("Go to -> http://localhost:4041/jobs")

print("Spark session initialized.")

start_time = time.time()
df = spark.read.format("bigquery") \
    .option("table", "bigquery-public-data.crypto_ethereum.transactions") \
    .option("filter", "block_timestamp >= TIMESTAMP('2024-05-01')") \
    .load()
print(f"Data loaded in {round(time.time() - start_time, 1)}s — Rows: {df.count():,}")

# Clean data
thief_wallet = "0x974caa59e49682cda0ad2bbe82983419a2ecc400"

df_cleaned = df.select("from_address", "to_address", "value", "gas_price", "block_timestamp") \
    .filter(col("from_address").isNotNull() & col("value").isNotNull() & (col("value") > 0))

df_thief_sent = df_cleaned.filter(col("from_address") == thief_wallet)
df_thief_received = df_cleaned.filter(col("to_address") == thief_wallet)

# Top 5 wallets by funds sent (by number of transactions and volume)
top_sent_wallets_count = df_thief_sent.groupBy("to_address") \
    .agg(count("*").alias("tx_count"), bround(sum("value"), 8).alias("total_value")) \
    .orderBy(desc("tx_count")) \
    .limit(5)

top_sent_wallets_value = df_thief_sent.groupBy("to_address") \
    .agg(bround(sum("value"), 8).alias("total_value"), count("*").alias("tx_count")) \
    .orderBy(desc("total_value")) \
    .limit(5)

# Top 5 wallets for receiving funds
top_received_wallets_count = df_thief_received.groupBy("from_address") \
    .agg(count("*").alias("tx_count"), bround(sum("value"), 8).alias("total_value")) \
    .orderBy(desc("tx_count")) \
    .limit(5)

top_received_wallets_value = df_thief_received.groupBy("from_address") \
    .agg(bround(sum("value"), 8).alias("total_value"), count("*").alias("tx_count")) \
    .orderBy(desc("total_value")) \
    .limit(5)

# General information
total_sent = df_thief_sent.agg(bround(sum("value"), 8).alias("total_sent"), count("*").alias("sent_count")).first()
total_received = df_thief_received.agg(bround(sum("value"), 8).alias("total_received"), count("*").alias("received_count")).first()

print(f"Total sent: {total_sent['total_sent']}, Total sent transactions: {total_sent['sent_count']}")
print(f"Total received: {total_received['total_received']}, Total received transactions: {total_received['received_count']}")

print("Top 5 wallets sent funds to (by count of transactions):")
top_sent_wallets_count.show(truncate=False)

print("Top 5 wallets sent funds to (by total value):")
top_sent_wallets_value.show(truncate=False)

print("Top 5 wallets received funds from (by count of transactions):")
top_received_wallets_count.show(truncate=False)

print("Top 5 wallets received funds from (by total value):")
top_received_wallets_value.show(truncate=False)

print("\nStopping Spark session...")
time.sleep(200)
spark.stop()
print("Process completed. Spark session closed.")
