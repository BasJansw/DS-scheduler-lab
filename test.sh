#!/bin/bash

# Default repeat value
REPEAT=1

# Parse command-line arguments for the -r flag
while getopts "r:" opt; do
    case $opt in
        r) REPEAT=$OPTARG ;;
        *) echo "Usage: $0 [-r REPEATS]" && exit 1 ;;
    esac
done

# Define the command with the repeat value
COMMAND="java -jar renaissance-gpl-0.16.0.jar reactors dotty -r $REPEAT"

# Run the command and capture the output
OUTPUT=$($COMMAND)

# Extract and list benchmark names with timings
echo "Benchmark Timings:" | tee results.txt
echo "$OUTPUT" | grep -E "====== .+ \[.+\], iteration [0-9]+ completed" | while read LINE; do
    BENCHMARK=$(echo "$LINE" | awk '{print $2}')  # Extract benchmark name
    TIME=$(echo "$LINE" | sed -E 's/.*\(([0-9.]+) ms\).*$/\1/')  # Extract timing
    echo "$BENCHMARK: $TIME ms" | tee -a results.txt
done
