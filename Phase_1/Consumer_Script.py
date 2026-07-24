from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import *
import os
import time
from datetime import datetime

# ==========================================================
# Configuration  (edit only this section)
# ==========================================================

TOPIC_NAME       = "aviation_project"
LANDING_PATH     = "hdfs:///user/hadoop/aviation/landing_zone"
CHECKPOINT_PATH  = "hdfs:///user/hadoop/aviation/checkpoint"
TRIGGER_INTERVAL = "5 seconds"
LOG_DIR          = "Logs"
LOG_FILE         = os.path.join(LOG_DIR, "consumer_summary.txt")
PRINT_EVERY      = 5   # print terminal output every N batches

# ==========================================================
# Ensure Log Directory Exists
# ==========================================================

os.makedirs(LOG_DIR, exist_ok=True)

# ==========================================================
# Spark Session
# ==========================================================

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("AviationKafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# Startup Banner
# ==========================================================

start_dt = datetime.now()

print("\n")
print("=" * 80)
print("              AVIATION KAFKA CONSUMER STARTED")
print("=" * 80)
print("Topic Name        :", TOPIC_NAME)
print("Landing Zone      :", LANDING_PATH)
print("Checkpoint        :", CHECKPOINT_PATH)
print("Trigger Interval  :", TRIGGER_INTERVAL)
print("Started At        :", start_dt.strftime("%d-%m-%Y %H:%M:%S"))
print("=" * 80)
print("Waiting For Kafka Messages...")
print("=" * 80)

# ==========================================================
# Kafka Stream
# ==========================================================

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe",               TOPIC_NAME) \
    .option("startingOffsets",         "earliest") \
    .option("failOnDataLoss",          "false") \
    .load()

# ==========================================================
# Flight Schema
# ==========================================================

flight_schema = ArrayType(
    StructType([
        StructField("FL_DATE",                StringType()),
        StructField("AIRLINE",               StringType()),
        StructField("AIRLINE_DOT",           StringType()),
        StructField("AIRLINE_CODE",          StringType()),
        StructField("DOT_CODE",              DoubleType()),
        StructField("FL_NUMBER",             DoubleType()),
        StructField("ORIGIN",                StringType()),
        StructField("ORIGIN_CITY",           StringType()),
        StructField("DEST",                  StringType()),
        StructField("DEST_CITY",             StringType()),
        StructField("CRS_DEP_TIME",          DoubleType()),
        StructField("DEP_TIME",              DoubleType()),
        StructField("DEP_DELAY",             DoubleType()),
        StructField("TAXI_OUT",              DoubleType()),
        StructField("WHEELS_OFF",            DoubleType()),
        StructField("WHEELS_ON",             DoubleType()),
        StructField("TAXI_IN",               DoubleType()),
        StructField("CRS_ARR_TIME",          DoubleType()),
        StructField("ARR_TIME",              DoubleType()),
        StructField("ARR_DELAY",             DoubleType()),
        StructField("CANCELLED",             DoubleType()),
        StructField("CANCELLATION_CODE",     StringType()),
        StructField("DIVERTED",              DoubleType()),
        StructField("CRS_ELAPSED_TIME",      DoubleType()),
        StructField("ELAPSED_TIME",          DoubleType()),
        StructField("AIR_TIME",              DoubleType()),
        StructField("DISTANCE",              DoubleType()),
        StructField("DELAY_DUE_CARRIER",     DoubleType()),
        StructField("DELAY_DUE_WEATHER",     DoubleType()),
        StructField("DELAY_DUE_NAS",         DoubleType()),
        StructField("DELAY_DUE_SECURITY",    DoubleType()),
        StructField("DELAY_DUE_LATE_AIRCRAFT", DoubleType()),
    ])
)

# ==========================================================
# Parse JSON From Kafka Value
# ==========================================================

parsed_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        flight_schema
    ).alias("flight_array")
)

# ==========================================================
# Explode Array → One Row Per Flight
# ==========================================================

landing_df = parsed_df.select(
    explode(col("flight_array")).alias("flight")
).select("flight.*")

# ==========================================================
# Running Totals  (mutated inside write_batch)
# ==========================================================

total_rows_processed    = 0
total_batches_processed = 0

# ==========================================================
# Batch Write Function
# ==========================================================

def write_batch(batch_df, batch_id):

    global total_rows_processed
    global total_batches_processed

    # --- Count & guard empty batches --------------------
    rows = batch_df.count()
    if rows == 0:
        return

    batch_start = time.time()

    total_rows_processed    += rows
    total_batches_processed += 1

    # --- Write to HDFS landing zone ---------------------
    batch_df.write \
        .mode("append") \
        .parquet(LANDING_PATH)

    batch_time = time.time() - batch_start
    elapsed    = time.time() - start_dt.timestamp()

    # --- Terminal output every PRINT_EVERY batches ------
    if total_batches_processed % PRINT_EVERY == 0:
        print("\n")
        print("=" * 80)
        print("Micro Batch Summary")
        print("=" * 80)
        print("Current Batch       :", batch_id)
        print("Rows In Batch       :", rows)
        print("Total Rows          :", total_rows_processed)
        print("Micro Batches       :", total_batches_processed)
        print("Current Batch Time  : {:.2f} Seconds".format(batch_time))
        print("Status              : ACTIVE")
        print("=" * 80)

    # --- Update consumer summary log --------------------
    with open(LOG_FILE, "w") as log:
        log.write("=" * 60 + "\n")
        log.write("       AVIATION KAFKA CONSUMER SUMMARY\n")
        log.write("=" * 60 + "\n")
        log.write("Topic                  : {}\n".format(TOPIC_NAME))
        log.write("Landing Zone           : {}\n".format(LANDING_PATH))
        log.write("Checkpoint             : {}\n".format(CHECKPOINT_PATH))
        log.write("Trigger                : {}\n".format(TRIGGER_INTERVAL))
        log.write("Micro Batches Processed: {}\n".format(total_batches_processed))
        log.write("Rows Consumed          : {}\n".format(total_rows_processed))
        log.write("Started At             : {}\n".format(start_dt.strftime("%d-%m-%Y %H:%M:%S")))
        log.write("Last Updated           : {}\n".format(datetime.now().strftime("%d-%m-%Y %H:%M:%S")))
        log.write("Elapsed Time           : {:.2f} Seconds\n".format(elapsed))
        log.write("Current Batch Time     : {:.2f} Seconds\n".format(batch_time))
        log.write("Status                 : ACTIVE\n")
        log.write("=" * 60 + "\n")

# ==========================================================
# Start Streaming Query
# ==========================================================

query = landing_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

# ==========================================================
# Keep Alive
# ==========================================================

query.awaitTermination()
