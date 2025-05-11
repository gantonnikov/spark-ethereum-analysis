import findspark
findspark.init("/opt/homebrew/Cellar/apache-spark/3.5.5/libexec/")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
import time
import psutil
import os

# Вывод информации о системе
print(f"Total CPU Cores Available: {os.cpu_count()}")

# Инициализация Spark (на 8 ядрах с оптимизацией)
spark = SparkSession.builder \
    .appName("WordCount_Spark") \
    .config("spark.sql.shuffle.partitions", 8) \
    .master("local[8]") \
    .getOrCreate()

# Отключение лишних предупреждений
spark.sparkContext.setLogLevel("ERROR")

# Замер времени и ресурсов
start_time = time.time()
start_cpu = psutil.cpu_percent()
start_mem = psutil.virtual_memory().percent

# Загрузка данных
data_path = "large_text_data.txt"  # Файл в текущей директории
df = spark.read.text(data_path).repartition(8)  # Принудительное распределение нагрузки

# Реализация WordCount
word_count = (
    df.withColumn("word", explode(split(col("value"), r"\s+")))
    .filter(col("word") != "")
    .coalesce(4)  # Балансировка для уменьшения избыточных потоков
    .groupBy("word")
    .count()
    .orderBy("count", ascending=False)
)

# Показ результата
word_count.show(20)

# Подсчёт дополнительных метрик
total_rows = df.count()
total_partitions = df.rdd.getNumPartitions()

# Замер времени и ресурсов
end_time = time.time()
end_cpu = psutil.cpu_percent()
end_mem = psutil.virtual_memory().percent

# Вывод результатов
print(f"\n✅ Total Rows Processed: {total_rows}")
print(f"✅ Number of Partitions: {total_partitions}")
print(f"✅ Total CPU Cores Used: 8")
print(f"\n🚀 Execution Time (Spark): {end_time - start_time:.4f} seconds")
print(f"🔥 CPU Usage (Spark): {end_cpu}%")
print(f"💾 RAM Usage (Spark): {end_mem}%")

spark.stop()
