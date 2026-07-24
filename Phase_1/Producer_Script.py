from kafka import KafkaProducer
import pandas as pd
import json
import os
import time
from datetime import datetime

# ==========================================================
# Configuration  (edit only this section)
# ==========================================================

TOPIC_NAME      = "aviation_project"
CSV_PATH        = "/home/talentum/Big_Data_Project_Work/Big_Data_Project/flights_sample_2m.csv"
CHUNK_SIZE      = 1000
DELAY_SECONDS   = 2
LOG_DIR         = "Logs"
LOG_FILE        = os.path.join(LOG_DIR, "producer_summary.txt")

# ==========================================================
# Ensure Log Directory Exists
# ==========================================================

os.makedirs(LOG_DIR, exist_ok=True)

# ==========================================================
# Kafka Producer
# ==========================================================

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer  = lambda x: json.dumps(x).encode("utf-8"),
    retries           = 5
)

# ==========================================================
# Startup Banner
# ==========================================================

start_time    = time.time()
start_dt      = datetime.now()
total_rows    = 0
total_batches = 0

print("\n")
print("=" * 80)
print("              AVIATION KAFKA PRODUCER STARTED")
print("=" * 80)
print("Topic Name      :", TOPIC_NAME)
print("CSV Path        :", CSV_PATH)
print("Batch Size      :", CHUNK_SIZE)
print("Producer Delay  :", str(DELAY_SECONDS) + " Seconds")
print("Started At      :", start_dt.strftime("%d-%m-%Y %H:%M:%S"))
print("=" * 80)

# ==========================================================
# Read CSV In Chunks And Produce To Kafka
# ==========================================================

for batch_id, chunk in enumerate(
    pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)
):
    # Replace NaN with None so JSON serialisation works cleanly
    chunk = chunk.where(pd.notnull(chunk), None)

    payload = chunk.to_dict(orient="records")

    future   = producer.send(TOPIC_NAME, value=payload)
    metadata = future.get(timeout=60)
    producer.flush()

    rows_sent      = len(payload)
    total_rows    += rows_sent
    total_batches += 1

    print("\n")
    print("=" * 80)
    print("Batch ID        :", batch_id)
    print("Rows Sent       :", rows_sent)
    print("Total Rows Sent :", total_rows)
    print("Partition       :", metadata.partition)
    print("Offset          :", metadata.offset)
    print("Status          : SUCCESS")
    print("=" * 80)

    time.sleep(DELAY_SECONDS)

# ==========================================================
# Close Producer
# ==========================================================

producer.close()

end_time  = time.time()
end_dt    = datetime.now()
elapsed   = end_time - start_time

# ==========================================================
# End Summary
# ==========================================================

print("\n")
print("=" * 80)
print("         ALL DATA SUCCESSFULLY PUSHED TO KAFKA")
print("=" * 80)
print("Total Batches   :", total_batches)
print("Total Rows      :", total_rows)
print("Execution Time  : {:.2f} Seconds".format(elapsed))
print("Completed At    :", end_dt.strftime("%d-%m-%Y %H:%M:%S"))
print("Status          : COMPLETED")
print("=" * 80)

# ==========================================================
# Write Producer Summary Log
# ==========================================================

with open(LOG_FILE, "w") as log:
    log.write("=" * 60 + "\n")
    log.write("        AVIATION KAFKA PRODUCER SUMMARY\n")
    log.write("=" * 60 + "\n")
    log.write("Topic             : {}\n".format(TOPIC_NAME))
    log.write("CSV               : {}\n".format(CSV_PATH))
    log.write("Batch Size        : {}\n".format(CHUNK_SIZE))
    log.write("Delay             : {} Seconds\n".format(DELAY_SECONDS))
    log.write("Rows Published    : {}\n".format(total_rows))
    log.write("Messages Published: {}\n".format(total_batches))
    log.write("Execution Time    : {:.2f} Seconds\n".format(elapsed))
    log.write("Start Time        : {}\n".format(start_dt.strftime("%d-%m-%Y %H:%M:%S")))
    log.write("End Time          : {}\n".format(end_dt.strftime("%d-%m-%Y %H:%M:%S")))
    log.write("Status            : COMPLETED\n")
    log.write("=" * 60 + "\n")

print("Log saved to      :", LOG_FILE)
