#!/bin/bash

URL="http://localhost:8080/metrics/put"

metric_names=("cpu_usage" "memory_usage" "disk_io" "network_latency" "service_latency")
hosts=("server1" "server2" "server3")
regions=("us-east" "us-west" "eu-central")
services=("auth" "payment" "checkout" "inventory")
endpoints=("/login" "/pay" "/cart" "/stock")

# Start from current time in ms
current_timestamp=$(date +%s%3N)

generate_metric() {
  local metricName="$1"
  local timestamp="$2"
  local host="${hosts[RANDOM % ${#hosts[@]}]}"
  local region="${regions[RANDOM % ${#regions[@]}]}"
  local value label_json

  if [[ "$metricName" == "service_latency" ]]; then
    local service="${services[RANDOM % ${#services[@]}]}"
    local endpoint="${endpoints[RANDOM % ${#endpoints[@]}]}"
    label_json=$(jq -n \
      --arg host "$host" \
      --arg region "$region" \
      --arg service "$service" \
      --arg endpoint "$endpoint" \
      '{
        host: $host,
        region: $region,
        service: $service,
        endpoint: $endpoint
      }')
    value=$(awk -v min=5 -v max=1000 'BEGIN{srand(); print int(min+rand()*(max-min))}')
  else
    label_json=$(jq -n \
      --arg host "$host" \
      --arg region "$region" \
      '{
        host: $host,
        region: $region
      }')
    value=$(awk -v min=0 -v max=100 'BEGIN{srand(); print min+rand()*(max-min)}')
  fi

  jq -n \
    --arg metricName "$metricName" \
    --argjson labels "$label_json" \
    --argjson timestamp "$timestamp" \
    --argjson value "$value" \
    '{
      metricName: $metricName,
      labels: $labels,
      timestamp: $timestamp,
      value: $value
    }'
}

generate_batch() {
  local n=$1
  local metrics=()
  for ((i=0; i<n; i++)); do
    local metric="${metric_names[RANDOM % ${#metric_names[@]}]}"
    # Increment current_timestamp by random 1-10 ms to simulate real-world time gaps
    current_timestamp=$((current_timestamp + RANDOM % 10 + 1))
    metrics+=("$(generate_metric "$metric" "$current_timestamp")")
  done
  printf '%s\n' "${metrics[@]}" | jq -s '.'
}

NUM_REQUESTS=1
BATCH_SIZE=20

for ((j=1; j<=NUM_REQUESTS; j++)); do
  batch_json=$(generate_batch $BATCH_SIZE)
  echo "Sending request #$j with $BATCH_SIZE metrics..."

  start_time=$(date +%s%3N)

  curl -s -X POST "$URL" \
    -H "Content-Type: application/json" \
    -d "$batch_json" > /dev/null

  end_time=$(date +%s%3N)
  duration=$((end_time - start_time))

  echo "Request #$j took ${duration}ms"
done
