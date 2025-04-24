#!/bin/bash

# Default parameters
BUFFER_POOL_SIZE=2500000
NUM_THREADS=1
WORKING_MEM=1000000
QUERY=100
SF=10
TARGET_RUN_COUNT=1200

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --working-mem=*)
      WORKING_MEM="${1#*=}"
      shift
      ;;
    --target-runcount=*)
      TARGET_RUN_COUNT="${1#*=}"
      shift
      ;;
    --threads=*)
      NUM_THREADS="${1#*=}"
      shift
      ;;
    --num-tuples=*)
      EXPLICIT_NUM_TUPLES="${1#*=}"
      shift
      ;;
    --buffer-pool=*)
      BUFFER_POOL_SIZE="${1#*=}"
      shift
      ;;
    --query=*)
      QUERY="${1#*=}"
      shift
      ;;
    --sf=*)
      SF="${1#*=}"
      shift
      ;;
    *)
      echo "Unknown parameter: $1"
      exit 1
      ;;
  esac
done

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export QUANTILE_METHOD=TPCH_100
export DATA_SOURCE="TPCH"
export SF=$SF
export QUERY_NUM=$QUERY

# Use explicit NUM_TUPLES if provided, otherwise calculate based on threads
if [ -n "$EXPLICIT_NUM_TUPLES" ]; then
  export NUM_TUPLES=$EXPLICIT_NUM_TUPLES
else
  export NUM_TUPLES=$((1501530 * NUM_THREADS))
fi

export TARGET_RUN_COUNT=$TARGET_RUN_COUNT

BP_DIR="bp-dir-tpch-sf-$SF"

# Print configuration for debugging
echo "Running with configuration:"
echo "  BUFFER_POOL_SIZE: $BUFFER_POOL_SIZE"
echo "  NUM_THREADS: $NUM_THREADS"
echo "  WORKING_MEM: $WORKING_MEM"
echo "  QUERY: $QUERY"
echo "  SF: $SF"
echo "  NUM_TUPLES: $NUM_TUPLES"
echo "  TARGET_RUN_COUNT: $TARGET_RUN_COUNT"
echo "  BP_DIR: $BP_DIR"

# Clean up previous runs
rm -rf "$BP_DIR/0/??*"

# Run the executable
cargo run --release --bin sort_run -- -q "$QUERY" -p "$BP_DIR" -n 1 -b "$BUFFER_POOL_SIZE"

# Clean up after
rm -rf "$BP_DIR/0/??*"
