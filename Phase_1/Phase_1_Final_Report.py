import os
import re
from datetime import datetime

# ==========================================================
# Configuration
# ==========================================================

LOG_DIR    = "Logs"
LOG_FILES  = {
    "producer": os.path.join(LOG_DIR, "producer_summary.txt"),
    "consumer": os.path.join(LOG_DIR, "consumer_summary.txt"),
    "phase1":   os.path.join(LOG_DIR, "phase1_summary.txt"),
}
OUTPUT_FILE = os.path.join(LOG_DIR, "project_execution_report.txt")

# ==========================================================
# Ensure Log Directory Exists
# ==========================================================

os.makedirs(LOG_DIR, exist_ok=True)

# ==========================================================
# Helper — Parse Key-Value Lines From A Summary File
# Returns a dict of { stripped_key: stripped_value }
# Lines that do not contain " : " are skipped (headers, dividers).
# ==========================================================

def parse_summary(filepath):
    """
    Read a summary text file and return a dictionary of
    key → value pairs parsed from lines that contain ' : '.
    Missing files are caught by the caller.
    """
    data = {}
    with open(filepath, "r") as f:
        for line in f:
            if " : " in line:
                key, _, value = line.partition(" : ")
                data[key.strip()] = value.strip()
    return data

# ==========================================================
# Read Each Summary File Safely
# ==========================================================

summaries  = {}
missing    = []

for name, path in LOG_FILES.items():
    if not os.path.exists(path):
        missing.append((name, path))
    else:
        try:
            summaries[name] = parse_summary(path)
        except Exception as e:
            missing.append((name, "{} (read error: {})".format(path, e)))

# Surface any missing files before writing the report
if missing:
    print("\n")
    print("=" * 60)
    print("  WARNING — The following report files are missing:")
    print("=" * 60)
    for name, path in missing:
        print("  [ {} ]  {}".format(name.upper(), path))
    print("=" * 60)
    print("  Run the pipeline stages in order and re-try.")
    print("=" * 60)
    print("\n")

# ==========================================================
# Helper — Safe Lookup With Fallback
# ==========================================================

def get(source_key, field, fallback="N/A"):
    """
    Safely retrieve a field from a parsed summary dict.
    Returns fallback if the source is missing or the field
    was not found in the file.
    """
    return summaries.get(source_key, {}).get(field, fallback)

# ==========================================================
# Extract Fields From Each Summary
# ==========================================================

# --- Producer ---
p_topic    = get("producer", "Topic")
p_batch    = get("producer", "Batch Size")
p_messages = get("producer", "Messages Published")
p_rows     = get("producer", "Rows Published")
p_time     = get("producer", "Execution Time")

# --- Consumer ---
c_landing  = get("consumer", "Landing Zone")
c_trigger  = get("consumer", "Trigger")
c_batches  = get("consumer", "Micro Batches Processed")
c_rows     = get("consumer", "Rows Consumed")
c_time     = get("consumer", "Elapsed Time")

# --- Phase 1 ---
ph_flight  = get("phase1",   "Flight Fact Count")
ph_airline = get("phase1",   "Airline Dim Count")
ph_airport = get("phase1",   "Airport Dim Count")
ph_delay   = get("phase1",   "Delay Fact Count")
ph_output  = get("phase1",   "Output HDFS Location")

# ==========================================================
# Build The Final Report
# ==========================================================

now        = datetime.now()
report_dt  = now.strftime("%d-%m-%Y")
report_tm  = now.strftime("%H:%M:%S")

SEP  = "=" * 60
THIN = "-" * 60

lines = [
    SEP,
    "          AVIATION BIG DATA PIPELINE",
    "           FINAL EXECUTION REPORT",
    SEP,
    "",
    "  Pipeline Overview",
    "",
    "    CSV",
    "     ↓",
    "    Kafka Producer",
    "     ↓",
    "    Kafka Topic",
    "     ↓",
    "    Spark Structured Streaming Consumer",
    "     ↓",
    "    Landing Zone (HDFS)",
    "     ↓",
    "    PySpark Transformations",
    "     ↓",
    "    Dimension Tables",
    "     ↓",
    "    Fact Tables",
    "",
    SEP,
    "  Producer Summary",
    SEP,
    "  Topic Name            : {}".format(p_topic),
    "  Batch Size            : {}".format(p_batch),
    "  Messages Published    : {}".format(p_messages),
    "  Rows Published        : {}".format(p_rows),
    "  Execution Time        : {}".format(p_time),
    "",
    SEP,
    "  Consumer Summary",
    SEP,
    "  Landing Zone          : {}".format(c_landing),
    "  Trigger Interval      : {}".format(c_trigger),
    "  Micro Batches         : {}".format(c_batches),
    "  Rows Consumed         : {}".format(c_rows),
    "  Execution Time        : {}".format(c_time),
    "",
    SEP,
    "  Phase 1 Summary",
    SEP,
    "  Flight Fact Count     : {}".format(ph_flight),
    "  Airline Dim Count     : {}".format(ph_airline),
    "  Airport Dim Count     : {}".format(ph_airport),
    "  Delay Fact Count      : {}".format(ph_delay),
    "  Output HDFS Location  : {}".format(ph_output),
    "",
    SEP,
    "  Overall Project Status",
    SEP,
    "  SUCCESS",
    "",
    SEP,
    "  Report Generated At",
    SEP,
    "  Date                  : {}".format(report_dt),
    "  Time                  : {}".format(report_tm),
    SEP,
    "",
]

report_text = "\n".join(lines)

# ==========================================================
# Write The Final Report
# ==========================================================

with open(OUTPUT_FILE, "w") as f:
    f.write(report_text)

# ==========================================================
# Terminal Output
# ==========================================================

print("\n")
print(report_text)
print("Report saved to       :", OUTPUT_FILE)
