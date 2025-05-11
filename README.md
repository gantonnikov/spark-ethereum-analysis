# Spark Ethereum Transaction Analysis

This repository contains Python scripts and analysis results using Apache Spark and Google BigQuery for large-scale Ethereum blockchain analytics, including forensic analysis of suspicious wallet activity.

## Project structure

- `cpypto_test/`: Spark scripts analyzing Ethereum blockchain:
  - `spark-for-bigquery.py`: 30-day Ethereum transaction analysis.
  - `spark_thiefwallet.py`: Annual suspicious wallet analysis.
  - Required dependencies and JAR connectors included.

- `wordcount_test/`: WordCount benchmark tests comparing Spark and Python performance.
  - `spark_wordcount.py`: Spark implementation.
  - `wordcount.py`: Standard Python implementation.
  - `gen.py`: Random text generator script.
  - `wordcount_results.txt`: Results of benchmark tests.

- `res/`: Screenshots and output logs from Spark jobs.

## Usage

See scripts in respective directories for detailed comments and execution instructions.

## Dependencies

- Apache Spark 3.5.1
- Python 3.11+
- Google Cloud BigQuery
