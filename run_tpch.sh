#!/bin/bash
# Parameters
BUFFER_POOL_SIZE=5000000
NUM_THREADS=1
# WORKING_MEM=142000
WORKING_MEM=1000000
QUERY=100
SF=10

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export QUANTILE_METHOD=TPCH_100
export DATA_SOURCE="TPCH"
export SF=$SF
export QUERY_NUM=$QUERY
export NUM_TUPLES=$((6005720 * SF))  
export NUM_TUPLES=$((1501530 * NUM_THREADS)) 
export TARGET_RUN_COUNT=1200

BP_DIR="bp-dir-tpch-sf-$SF"

# Clean up previous runs
rm -rf "$BP_DIR/0/??*"

cargo run --release --bin sort_run -- -q "$QUERY" -p "$BP_DIR" -n 1 -b "$BUFFER_POOL_SIZE" 

rm -rf "$BP_DIR/0/??*"
