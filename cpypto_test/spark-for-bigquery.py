import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, desc

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "spark-access-459420-f8b1f097831d.json"

# Start Spark session with a fixed UI port for monitoring
print("Initializing Spark session...")
try:
    spark = SparkSession.builder \
        .appName("Ethereum5DaysAnalysis") \
        .config("spark.ui.port", "4041") \
        .config("spark.jars", "spark-bigquery-with-dependencies_2.12-0.32.0.jar") \
        .getOrCreate() 
    print("Spark session initialized successfully.")
    print("Go to -> http://localhost:4041/jobs/")
except Exception as e:
    print(f"Error initializing Spark session: {e}")
    exit(1)

print("Starting data load from BigQuery...")
start_time = time.time()
try:
    df = spark.read.format("bigquery") \
        .option("table", "bigquery-public-data.crypto_ethereum.transactions") \
        .option("filter", "block_timestamp >= TIMESTAMP('2025-04-09') AND block_timestamp < TIMESTAMP('2025-05-09')") \
        .load()
    print(f"Data loaded in {round(time.time() - start_time, 1)}s — Rows: {df.count():,}")
except Exception as e:
    print(f"Error loading data from BigQuery: {e}")
    spark.stop()
    exit(1)

# Data selection and filtering
print("Processing and cleaning data...")
df_cleaned = df.select("from_address", "to_address", "value", "gas_price", "block_timestamp") \
              .filter(col("from_address").isNotNull())

# Analysis: 1. Top 10 senders by transaction count
print("Performing analysis: Top 10 senders by transaction count...")
top_senders_count = df_cleaned.groupBy("from_address") \
    .agg(count("*").alias("tx_count")) \
    .orderBy(desc("tx_count")) \
    .limit(10)

# Analysis: 2. Top 10 by total value
print("Performing analysis: Top 10 senders by total value...")
top_senders_value = df_cleaned.groupBy("from_address") \
    .agg(sum("value").alias("total_value")) \
    .orderBy(desc("total_value")) \
    .limit(10)

# Analysis: 3. Average gas price per sender
print("Performing analysis: Top 10 senders by average gas price...")
avg_gas_price = df_cleaned.groupBy("from_address") \
    .agg(avg("gas_price").alias("avg_gas_price")) \
    .orderBy(desc("avg_gas_price")) \
    .limit(10)

# Analysis: 4. Count of "expensive" transactions (gas_price > 10^9)
print("Counting expensive transactions...")
expensive_tx = df_cleaned.filter(col("gas_price") > 10**9).count()

# Output results
print("\nAnalysis Results:")
print("Top 10 senders by transaction count:")
top_senders_count.show(truncate=False)

print("Top 10 senders by total value:")
top_senders_value.show(truncate=False)

print("Top 10 senders by average gas price:")
avg_gas_price.show(truncate=False)

print(f"Expensive transactions (gas_price > 10^9): {expensive_tx:,}")

# Stop Spark session
print("\nStopping Spark session...")
time.sleep(60)
spark.stop()
print("Process completed. Spark session closed.")