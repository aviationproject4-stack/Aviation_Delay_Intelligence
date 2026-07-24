#!/bin/bash

echo "=============================================================="
echo "        STARTING AVIATION STREAMING PROJECT"
echo "=============================================================="

PROJECT_DIR="$HOME/Big_Data_Project_Work/Big_Data_Project"

echo ""
echo "Starting Consumer..."

gnome-terminal -- bash -c "
source ~/unset_jupyter.sh
cd $PROJECT_DIR

echo 'Starting Spark Consumer...'
echo

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
Consumer_Script.py

exec bash
"

echo ""
echo "Waiting 10 seconds for Consumer to initialize..."

sleep 10

echo ""
echo "Starting Producer..."

gnome-terminal -- bash -c "
source ~/unset_jupyter.sh
cd $PROJECT_DIR

echo 'Starting Kafka Producer...'
echo

python3 Producer_Script.py

exec bash
"

echo ""
echo "=============================================================="
echo "Consumer Started"
echo "Producer Started"
echo "Streaming Pipeline Running..."
echo "=============================================================="
