#!/bin/bash

# ==========================================================
# Configuration
# ==========================================================

HDFS_SOURCE="/user/hadoop/aviation/final_parquets"
LOCAL_DEST="/home/talentum/Big_Data_Project_Work/Big_Data_Project/Phase1_Output"
SHARED_DEST="/home/talentum/shared/Big_Data_Project"

# ==========================================================
# Header
# ==========================================================

echo ""
echo "======================================================"
echo "      Exporting Final Parquets From HDFS"
echo "======================================================"

# ==========================================================
# Prepare Local Destination
# ==========================================================

echo ""
echo "Removing Previous Export..."
rm -rf "$LOCAL_DEST"
mkdir -p "$LOCAL_DEST"

# ==========================================================
# Copy From HDFS To Local
# ==========================================================

echo ""
echo "Copying Airline Dimension..."
hdfs dfs -get \
    $HDFS_SOURCE/airline_dim \
    $LOCAL_DEST/

echo "Copying Airport Dimension..."
hdfs dfs -get \
    $HDFS_SOURCE/airport_dim \
    $LOCAL_DEST/

echo "Copying Delay Fact..."
hdfs dfs -get \
    $HDFS_SOURCE/delay_fact \
    $LOCAL_DEST/

echo "Copying Flight Fact..."
hdfs dfs -get \
    $HDFS_SOURCE/flight_fact \
    $LOCAL_DEST/

echo ""
echo "======================================================"
echo "        EXPORT COMPLETED SUCCESSFULLY"
echo "======================================================"

# ==========================================================
# Copy To Shared Location
# ==========================================================

echo ""
echo "======================================================"
echo "      Copying Parquets To Shared Location"
echo "======================================================"
echo ""
echo "Destination : $SHARED_DEST"
echo ""

echo "Removing Previous Shared Copy..."

rm -rf "$SHARED_DEST"

mkdir -p "$SHARED_DEST"

echo "Copying Airline Dimension..."
cp -r "$LOCAL_DEST/airline_dim"  "$SHARED_DEST/"

echo "Copying Airport Dimension..."
cp -r "$LOCAL_DEST/airport_dim"  "$SHARED_DEST/"

echo "Copying Delay Fact..."
cp -r "$LOCAL_DEST/delay_fact"   "$SHARED_DEST/"

echo "Copying Flight Fact..."
cp -r "$LOCAL_DEST/flight_fact"  "$SHARED_DEST/"

echo ""
echo "======================================================"
echo "         SHARED COPY COMPLETED SUCCESSFULLY"
echo "======================================================"

# ==========================================================
# Summary
# ==========================================================

#echo ""
#echo "======================================================"
#echo "  Local Export"
#echo "======================================================"
#ls -R "$LOCAL_DEST"

#echo ""
#echo "======================================================"
#echo "  Shared Location"
#echo "======================================================"
#ls -R "$SHARED_DEST"

#echo ""
#echo "======================================================"
