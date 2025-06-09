# Databricks notebook source
# DBTITLE 1,import
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from io import StringIO
import time
import os
import zipfile
import boto3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import tempfile
import pandas as pd
import time
import uuid
import os
import pytz
from datetime import datetime
import psycopg2
import time
import urllib.parse
# Setup Spark session
spark = SparkSession.builder.appName("ChunkUploader").getOrCreate()
from pyspark.sql.functions import broadcast



# COMMAND ----------

# DBTITLE 1,constant
INPUT_PATH = "/Volumes/transaction/temp/transactions"
CHUNK_SIZE = 10000
AWS_ACCESS_KEY_ID = <AWS_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY = <AWS_SECRET_ACCESS_KEY>
S3_BUCKET = 'pyspark-transaction-chunks'
S3_PREFIX = "transactions_chunks/"
CHUNK_DIR = "/dbfs/tmp/transactions/"
DETECTION_DIR = "/dbfs/tmp/detections/"


# COMMAND ----------

# DBTITLE 1,s3 initialize
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="ap-south-1"
)

# COMMAND ----------

# DBTITLE 1,read transaction file
# File location and type
file_location = "/FileStore/tables/transactions.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# Load the CSV as a Spark DataFrame
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df_with_id = df.withColumn("row_id", monotonically_increasing_id())

# Get total number of rows (approximate)
total_rows = df_with_id.count()
print(f"Total rows in transactions: {total_rows}")

chunk_size = 10000
num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)

print(f"Total chunks: {num_chunks}")

# COMMAND ----------

# DBTITLE 1,upload s3
def upload_spark_chunk_to_s3(spark_df_chunk, chunk_index):
    """
    Uploads a Spark DataFrame chunk to S3 as CSV using boto3
    by converting Spark DF to Pandas.
    """
    # Convert Spark DataFrame chunk to Pandas
    pandas_df = spark_df_chunk.drop("row_id").toPandas()

    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)

    s3_key = f"{S3_PREFIX}transactions_chunk_{chunk_index}.csv"
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())

    print(f"Uploaded chunk {chunk_index} to s3://{S3_BUCKET}/{s3_key}")

# COMMAND ----------

def current_ist_time():
    return datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()

# COMMAND ----------

# DBTITLE 1,postgre table
def setup_postgres_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS merchant_tx_count (
                merchant_id TEXT PRIMARY KEY,
                tx_count BIGINT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_chunks (
                file_name TEXT PRIMARY KEY
            );
        """)
    conn.commit()


# COMMAND ----------

# DBTITLE 1,detect pattern
from pyspark.sql.functions import col, count, sum as _sum, mean as _mean, expr, when
from pyspark.sql import Window

def detect_patterns_spark(df_spark, customer_df_spark, y_start_time, pg_conn):
    required_columns = {'merchant_id', 'customer_name', 'amount'}
    if not required_columns.issubset(set(df_spark.columns)):
        missing = required_columns - set(df_spark.columns)
        raise ValueError(f"Missing columns: {missing}")

    customer_df_spark = customer_df_spark.withColumnRenamed("Target", "merchant_id")
    if 'Weight' not in customer_df_spark.columns or 'merchant_id' not in customer_df_spark.columns:
        raise ValueError("customer_df must have 'merchant_id' and 'Weight' columns")

    # Merge weight
    df_spark = df_spark.join(customer_df_spark.select("merchant_id", "Weight"), on="merchant_id", how="left")
    df_spark = df_spark.withColumn("Weight", col("Weight").cast("double")).na.fill({"Weight": 0})

    detections = []

    # Pattern 1 - transaction count per merchant (Postgres logic retained)
    merchant_counts = df_spark.groupBy("merchant_id").agg(count("*").alias("tx_count")).collect()
    with pg_conn.cursor() as cursor:
        insert_query = """
            INSERT INTO merchant_tx_count (merchant_id, tx_count) 
            VALUES %s
            ON CONFLICT (merchant_id) DO UPDATE 
            SET tx_count = merchant_tx_count.tx_count + EXCLUDED.tx_count
        """
        from psycopg2.extras import execute_values
        data = [(row['merchant_id'], row['tx_count']) for row in merchant_counts]
        execute_values(cursor, insert_query, data)
        pg_conn.commit()
        
    merchant_counts_dict = {row['merchant_id']: row['tx_count'] for row in merchant_counts}
    eligible_merchants = [m for m, count in merchant_counts_dict.items() if count > 50000]

    # Pattern 1 continued
    eligible_df = df_spark.filter(col("merchant_id").isin(eligible_merchants))
    
    if not eligible_df.rdd.isEmpty():
        tx_counts_df = eligible_df.groupBy("merchant_id", "customer_name").agg(
            count("*").alias("tx_count"),
            _sum("Weight").alias("weight_sum")
        )

        window_tx = Window.partitionBy("merchant_id")
        window_wt = Window.partitionBy("merchant_id")
        # Collect merchant list
        grouped_stats_df = tx_counts_df.groupBy("merchant_id").agg(
            expr("percentile_approx(tx_count, 0.99)").alias("tx_quantile_99"),
            expr("percentile_approx(weight_sum, 0.01)").alias("wt_quantile_01")
        )

        # Join and filter based on per-merchant quantiles
        filtered_df = tx_counts_df.join(broadcast(grouped_stats_df), on="merchant_id", how="inner") \
            .filter((col("tx_count") >= col("tx_quantile_99")) & (col("weight_sum") <= col("wt_quantile_01")))

        results = filtered_df.select("customer_name", "merchant_id").collect()
        for row in results:
            detections.append([y_start_time, current_ist_time(), "PatId1", "UPGRADE", row['customer_name'], row['merchant_id']])

    # Pattern 2 - mean < 23 and count >= 80
    stats_df = df_spark.groupBy("merchant_id", "customer_name").agg(
        _mean("amount").alias("mean_amount"),
        count("*").alias("tx_count")
    ).filter((col("mean_amount") < 23) & (col("tx_count") >= 80))

    for row in stats_df.collect():
        detections.append([y_start_time, current_ist_time(), "PatId2", "CHILD", row['customer_name'], row['merchant_id']])

    # Pattern 3 - gender DEI
    if 'gender' in df_spark.columns:
        gender_df = df_spark.groupBy("merchant_id", "gender").agg(count("*").alias("count"))
        gender_pivot = gender_df.groupBy("merchant_id").pivot("gender", ["F", "M"]).agg(expr("first(count)")).na.fill(0)

        gender_pivot = gender_pivot.filter((col("F") > 100) & (col("F") < col("M")))
        for row in gender_pivot.select("merchant_id").collect():
            detections.append([y_start_time, current_ist_time(), "PatId3", "DEI-NEEDED", "", row["merchant_id"]])

    return detections


# COMMAND ----------

S3_OUTPUT_BUCKET = "pyspark-transaction-chunks"
S3_OUTPUT_PREFIX = "detections_output"

def upload_to_s3(local_path, s3_key):
    s3_client.upload_file(local_path, S3_OUTPUT_BUCKET, s3_key)
    print(f"[✓] Uploaded to S3: s3://{S3_OUTPUT_BUCKET}/{s3_key}")

# COMMAND ----------

# DBTITLE 1,main function
os.makedirs(DETECTION_DIR, exist_ok=True)
# Your full connection URL (replace with your actual credentials)
DATABASE_URL = (
    "postgresql://neondb_owner:npg_t0hT5sjUXeSc@ep-misty-silence-a18y6tmf-pooler.ap-southeast-1.aws.neon.tech/"
    "neondb?sslmode=require&options=endpoint%3Dep-misty-silence-a18y6tmf"
)

def create_pg_connection():
    return psycopg2.connect(DATABASE_URL)

def is_connection_alive(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except:
        return False

# Create initial connection
pg_conn = create_pg_connection()
setup_postgres_tables(pg_conn)

customer_df_spark = spark.read.csv("dbfs:/FileStore/tables/CustomerImportance.csv", header=True, inferSchema=True)

for chunk_index in range(num_chunks):
    start_row = chunk_index * CHUNK_SIZE
    end_row = start_row + CHUNK_SIZE

    # Upload chunk to S3
    chunk_df = df_with_id.filter((df_with_id.row_id >= start_row) & (df_with_id.row_id < end_row))
    upload_spark_chunk_to_s3(chunk_df, chunk_index)

    s3_chunk_path = f"s3://pyspark-transaction-chunks/transactions_chunks/transactions_chunk_{chunk_index}.csv"

    # Check & reconnect if needed
    if not is_connection_alive(pg_conn):
        print("DB connection lost. Reconnecting...")
        try:
            pg_conn.close()
        except:
            pass
        pg_conn = create_pg_connection()

    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT file_name FROM processed_chunks")
            processed_files = [row[0] for row in cursor.fetchall()]

        if s3_chunk_path not in processed_files:
            # Read the chunk CSV from S3 (use spark or boto3)
            spark_chunk_df = spark.read.option("header", "true").csv(s3_chunk_path)
            #print(f"Columns in chunk before rename {chunk_index}: {spark_chunk_df.columns.tolist()}")
            spark_chunk_df = spark_chunk_df.withColumnRenamed("merchant", "merchant_id") \
                                           .withColumnRenamed("customer", "customer_name")
            #print(f"Columns in chunk {chunk_index}: {spark_chunk_df.columns.tolist()}")
            spark_chunk_df = spark_chunk_df.withColumn("amount", col("amount").cast("double"))
            spark_chunk_df = spark_chunk_df.filter(col("amount").isNotNull())
            y_start_time = current_ist_time()
            print(f"[{chunk_index}] Starting detection pattern evaluation...")
            detections = detect_patterns_spark(spark_chunk_df, customer_df_spark, y_start_time, pg_conn)
            print(f"[{chunk_index}] Detection returned {len(detections)} records.")
            for i in range(0, len(detections), 50):
                chunk = detections[i:i + 50]
                out_df = pd.DataFrame(chunk, columns=["YStartTime", "detectionTime", "patternId", "ActionType", "customerName", "MerchantId"])
                file_name = f"detection_{uuid.uuid4().hex}.csv"
                local_path = os.path.join(DETECTION_DIR, file_name)
                out_df.to_csv(os.path.join(DETECTION_DIR, file_name), index=False)
                print(f"[Y] Wrote detection chunk: {file_name}")
                s3_key = f"{S3_OUTPUT_PREFIX}/{file_name}"
                upload_to_s3(local_path, s3_key)

            with pg_conn.cursor() as cursor:
                cursor.execute("INSERT INTO processed_chunks (file_name) VALUES (%s)", (s3_chunk_path,))
            pg_conn.commit()

    except Exception as e:
        print(f"Error during processing chunk {chunk_index}: {e}")

    time.sleep(1)


# COMMAND ----------

# DBTITLE 1,output data
import pandas as pd
import os

# List local detection files
for file in os.listdir(DETECTION_DIR):
    if file.endswith(".csv"):
        print(file)

# Read one sample file
sample_df = pd.read_csv(os.path.join(DETECTION_DIR, file))
print(sample_df.head())


# COMMAND ----------

# DBTITLE 1,intermediary files
import boto3

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='ap-south-1'  # Change as per your region
)

s3 = session.client('s3')
bucket = 'pyspark-transaction-chunks'
prefix = 'transactions_chunks/'

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

print("Transaction Chunks in S3:")
for obj in response.get('Contents', []):
    print(obj['Key'])


# COMMAND ----------

# DBTITLE 1,postgre
import psycopg2

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Example: View processed chunk entries
cur.execute("SELECT * FROM processed_chunks LIMIT 5")
for row in cur.fetchall():
    print(row)

# Example: View merchant transaction counts
cur.execute("SELECT * FROM merchant_tx_count ORDER BY tx_count DESC LIMIT 5")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
