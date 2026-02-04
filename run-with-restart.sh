#!/bin/bash
# Auto-restart wrapper for the indexer
# Restarts on failure (network errors, etc.)

MAX_RETRIES=100
RETRY_DELAY=5

for i in $(seq 1 $MAX_RETRIES); do
    echo "=== Starting indexer (attempt $i/$MAX_RETRIES) ==="
    npm run start
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "Indexer completed successfully"
        exit 0
    fi

    echo "Indexer exited with code $EXIT_CODE, restarting in ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY
done

echo "Max retries reached"
exit 1
