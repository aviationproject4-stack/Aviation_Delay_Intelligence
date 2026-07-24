#!/bin/bash

echo "======================================================"
echo "Resetting Aviation Big Data Project"
echo "======================================================"

echo ""
echo "Stopping Previous Consumer (if running)..."
pkill -f Consumer_Script.py

echo ""
echo "Deleting HDFS Landing Zone..."

hdfs dfs -rm -r -skipTrash /user/hadoop/aviation/landing_zone

echo ""
echo "Deleting HDFS Checkpoint..."

hdfs dfs -rm -r -skipTrash /user/hadoop/aviation/checkpoint

echo ""
echo "Creating Fresh HDFS Directories..."

hdfs dfs -mkdir -p /user/hadoop/aviation/landing_zone

hdfs dfs -mkdir -p /user/hadoop/aviation/checkpoint

echo ""
echo "Deleting Kafka Topic..."

kafka-topics.sh \
--delete \
--topic aviation_project \
--bootstrap-server localhost:9092

sleep 5

echo ""
echo "Creating Kafka Topic..."

kafka-topics.sh \
--create \
--topic aviation_project \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

echo ""
echo "Current Topics"

kafka-topics.sh \
--list \
--bootstrap-server localhost:9092

echo ""
echo "Checking HDFS"

hdfs dfs -ls /user/hadoop/aviation

echo ""
echo "======================================================"
echo "PROJECT RESET SUCCESSFULLY"
echo "======================================================"
