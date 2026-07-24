#!/bin/bash

# ==========================================================
# Configuration
# ==========================================================

SHARED_DIR="/home/talentum/shared/Big_Data_Project"
ZIP_DIR="$SHARED_DIR/Zipped"

# ==========================================================
# Header
# ==========================================================

echo ""
echo "======================================================"
echo "       Zipping Parquet Folders In Shared Location"
echo "======================================================"
echo ""
echo "Source      : $SHARED_DIR"
echo "Destination : $ZIP_DIR"
echo ""

# ==========================================================
# Prepare Zip Output Directory
# ==========================================================

mkdir -p "$ZIP_DIR"

# ==========================================================
# Zip Each Parquet Folder
# ==========================================================

FOLDERS=("airline_dim" "airport_dim" "delay_fact" "flight_fact")

for FOLDER in "${FOLDERS[@]}"; do

    SRC="$SHARED_DIR/$FOLDER"
    OUT="$ZIP_DIR/${FOLDER}.zip"

    if [ -d "$SRC" ]; then
        echo "Zipping $FOLDER..."
        zip -rq "$OUT" "$SRC"
        echo "Done : $OUT"
        echo ""
    else
        echo "WARNING : $SRC not found — skipping."
        echo ""
    fi

done

# ==========================================================
# Summary
# ==========================================================

echo "======================================================"
echo "           ZIP COMPLETED SUCCESSFULLY"
echo "======================================================"
echo ""
echo "Zipped Files"
ls -lh "$ZIP_DIR"
echo ""
echo "======================================================"
