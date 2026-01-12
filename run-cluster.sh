#!/usr/bin/env bash
set -ex

HDFS_PREFIX=/user/khaser
HDFS_INPUT_DIR=$HDFS_PREFIX/input_data
HDFS_BASE_OUTPUT_DIR=$HDFS_PREFIX/output
HDFS_BASE_OUTPUT_SORTED_DIR=$HDFS_PREFIX/output_sorted
JAR_FILE=./build/libs/hw3-hadoop-1.0-SNAPSHOT.jar
DRIVER_CLASS=Main
REPORT=final_report.txt

# LOCAL_INPUT_DIR=./data
# hdfs dfs -rm -r -f $HDFS_INPUT_DIR/
# hdfs dfs -mkdir -p $HDFS_INPUT_DIR
# hdfs dfs -put -f $LOCAL_INPUT_DIR/* $HDFS_INPUT_DIR/

[ -f "$FINAL_REPORT" ] && rm $FINAL_REPORT

[ ! -f "$JAR_FILE" ] && echo "JAR not found" && exit 1

hdfs dfs -rm -r -f ${HDFS_BASE_OUTPUT_DIR}_*
rm -rf logs && mkdir logs

for reducers in 1 2 4 8; do
    HDFS_OUTPUT_DIR="${HDFS_BASE_OUTPUT_DIR}_reducers_${reducers}"
    HDFS_OUTPUT_SORTED_DIR="${HDFS_BASE_OUTPUT_SORTED_DIR}_reducers_${reducers}"

    echo "Starting with $reducers reducers"
    hadoop jar $JAR_FILE $DRIVER_CLASS $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $HDFS_OUTPUT_SORTED_DIR $reducers

    TEMP_FILE="temp_output.txt"
    hdfs dfs -getmerge $HDFS_OUTPUT_SORTED_DIR $TEMP_FILE
    {
        echo "With $reducers reducers"
        echo "		Category		Revenue;		Quantity;"
        cat $TEMP_FILE
        echo ""
    } >> $REPORT
    rm $TEMP_FILE
done

echo "Benchmarking report: $REPORT"
