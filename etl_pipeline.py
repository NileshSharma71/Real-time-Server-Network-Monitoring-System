import os
import sys
from datetime import datetime 

# --- 1. JAVA & HADOOP SETUP ---
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.10.7-hotspot"
os.environ["JAVA_HOME"] = java_path
os.environ["PATH"] = java_path + r"\bin;" + os.environ["PATH"]

current_dir = os.getcwd()
hadoop_home = os.path.join(current_dir, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["hadoop.home.dir"] = hadoop_home
os.environ["PATH"] = os.path.join(hadoop_home, "bin") + ";" + os.environ["PATH"]

# --- 2. START SPARK ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, from_unixtime, round

spark = SparkSession.builder \
    .appName("GlobalServerETL") \
    .config("spark.jars", "drivers/postgresql-42.7.2.jar") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(">>> [ETL] Spark Session Started. Reading Data...")

# --- 3. EXTRACT ---
df_ind = spark.read.csv("data/india_lab/*.csv", header=True, inferSchema=True)
df_us = spark.read.json("data/us_cloud/*.json")
df_ger = spark.read.parquet("data/germany_hpc/*.parquet")

# --- 4. TRANSFORM ---
# India
df_ind_clean = df_ind.select(
    col("server_code").alias("host_id"),
    to_timestamp(col("timestamp_ist")).alias("timestamp"),
    col("temp_celsius").alias("temp_c"), 
    col("cpu_usage_percent").alias("cpu_load"),
    lit(None).cast("double").alias("voltage"),
    lit(None).cast("double").alias("power_watts")
)
# USA
df_us_clean = df_us.withColumn("temp_c", round((col("temp_fahrenheit") - 32) * 5/9, 1)) \
    .withColumn("cpu_load", col("load_fraction") * 100) \
    .select(
        col("instance_id").alias("host_id"),
        to_timestamp(col("time_utc")).alias("timestamp"),
        "temp_c",
        "cpu_load",
        col("voltage_v").alias("voltage"),
        lit(None).cast("double").alias("power_watts")
    )

# Germany
df_ger_clean = df_ger.withColumn("temp_c", round(col("temp_kelvin") - 273.15, 1)) \
    .withColumn("timestamp", from_unixtime(col("epoch_time")).cast("timestamp")) \
    .select(
        col("node_name").alias("host_id"),
        "timestamp",
        "temp_c",
        lit(None).cast("double").alias("cpu_load"),
        lit(None).cast("double").alias("voltage"),
        col("power_watts")
    )

# Merge All
df_final = df_ind_clean.union(df_us_clean).union(df_ger_clean)

# --- OPTIMIZATION: Cache data in memory ---
df_final.cache()

print(f">>> [ETL] Transformation Complete. Rows to process: {df_final.count()}")

# --- 5. LOAD 1: COLD STORAGE (JSON ARCHIVE) ---
# NEW LOGIC: Create a timestamped folder inside 'data/batch_data/'
run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
archive_path = f"data/output/batch_data/batch_run_{run_id}"

print(f">>> [Sink 1] Archiving Batch data to: {archive_path}")
df_final.write.mode("append").json(archive_path)

# --- 6. LOAD 2: HOT STORAGE (DATABASE) ---
print(">>> [Sink 2] Writing data to PostgreSQL...")

db_properties = {
    "user": "postgres",
    "password": "password", # <--- check the password before running
    "driver": "org.postgresql.Driver"
}

df_final.write.jdbc(
    url="jdbc:postgresql://localhost:5432/bigdata_db",
    table="server_metrics",
    mode="append",
    properties=db_properties
)

print(">>> [✓] SUCCESS: Data pipeline execution finished.")