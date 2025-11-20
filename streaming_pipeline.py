import os
import sys
from datetime import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# --- 1. SETUP ---
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.10.7-hotspot"
os.environ["JAVA_HOME"] = java_path
os.environ["PATH"] = java_path + r"\bin;" + os.environ["PATH"]

current_dir = os.getcwd()
hadoop_home = os.path.join(current_dir, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["hadoop.home.dir"] = hadoop_home
os.environ["PATH"] = os.path.join(hadoop_home, "bin") + ";" + os.environ["PATH"]

# --- CONFIG: RUN ID FOR THIS SESSION ---
# This creates a unique folder name for THIS run (e.g., stream_run_2025...)
run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# Define the absolute path: Desktop/Big_data/data/live_data/stream_run_...
base_dir = os.getcwd()
session_folder = os.path.join(base_dir, "data","output", "live_data", f"stream_run_{run_id}")

# Create the folder immediately
if not os.path.exists(session_folder):
    os.makedirs(session_folder)

print(f">>> [Setup] Live data for this session will be saved to: {session_folder}")


# --- 2. INIT SPARK ---
spark = SparkSession.builder \
    .appName("LiveServerStream") \
    .config("spark.jars", "drivers/postgresql-42.7.2.jar") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(">>> [Stream] Spark Streaming Started. Listening on 127.0.0.1:9988...")

# --- 3. READ STREAM ---
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 9988) \
    .load()

# --- 4. TRANSFORM ---
json_schema = StructType() \
    .add("host_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("temp_c", IntegerType()) \
    .add("cpu_load", IntegerType()) \
    .add("voltage", DoubleType()) \
    .add("power_watts", DoubleType())

parsed_stream = raw_stream.select(
    from_json(col("value"), json_schema).alias("data")
).select(
    col("data.host_id"),
    to_timestamp(col("data.timestamp")).alias("timestamp"),
    col("data.temp_c").cast("double").alias("temp_c"),
    col("data.cpu_load").cast("double").alias("cpu_load"),
    col("data.voltage"),
    col("data.power_watts")
)

# --- 5. DUAL SINK (Database + FOLDER ARCHIVE) ---
def process_batch(df, epoch_id):
    print(f">>> [Stream] Processing Batch {epoch_id}...")
    df.cache()
    
    # A. Write to DB
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/bigdata_db") \
            .option("dbtable", "server_metrics") \
            .option("user", "postgres") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"    [!] DB Error: {e}")

    # B. Write to Archive (Inside the unique session folder)
    try:
        pdf = df.toPandas()
        if not pdf.empty:
            # We save to 'stream_data.json' INSIDE the timestamped folder
            file_path = os.path.join(session_folder, "stream_data.json")
            
            with open(file_path, "a") as f:
                f.write(pdf.to_json(orient='records', lines=True))
                f.write("\n")
            print(f"    [✓] Archived {len(pdf)} rows to folder: .../{os.path.basename(session_folder)}")
    except Exception as e:
        print(f"    [!] Archive Error: {e}")
    
    df.unpersist()

# Start Stream
query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()