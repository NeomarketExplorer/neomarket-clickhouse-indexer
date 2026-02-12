#!/bin/bash
# Auto-restart wrapper for the indexer
# Restarts on failure (network errors, etc.)

MAX_RETRIES=100
RETRY_DELAY=5

# Build endpoint rotation list from RPC_ENDPOINTS, then RPC_ENDPOINT.
IFS=',' read -r -a RAW_RPC_ENDPOINTS <<< "${RPC_ENDPOINTS:-}"
RPC_ENDPOINTS_LIST=()

for endpoint in "${RAW_RPC_ENDPOINTS[@]}"; do
    trimmed="$(echo "$endpoint" | xargs)"
    if [ -n "$trimmed" ]; then
        RPC_ENDPOINTS_LIST+=("$trimmed")
    fi
done

if [ "${#RPC_ENDPOINTS_LIST[@]}" -eq 0 ]; then
    if [ -n "${RPC_ENDPOINT:-}" ]; then
        RPC_ENDPOINTS_LIST+=("${RPC_ENDPOINT}")
    else
        RPC_ENDPOINTS_LIST+=("https://polygon-rpc.com")
        RPC_ENDPOINTS_LIST+=("https://polygon.drpc.org")
        RPC_ENDPOINTS_LIST+=("https://polygon-bor-rpc.publicnode.com")
    fi
fi

for i in $(seq 1 $MAX_RETRIES); do
    endpoint_index=$(( (i - 1) % ${#RPC_ENDPOINTS_LIST[@]} ))
    export RPC_ENDPOINT="${RPC_ENDPOINTS_LIST[$endpoint_index]}"

    echo "=== Starting indexer (attempt $i/$MAX_RETRIES) ==="
    echo "RPC endpoint: $RPC_ENDPOINT"
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
