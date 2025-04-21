#!/usr/bin/env bash

# Number of test cycles
N=5

export TEST_ID=4
export TOTAL_ORDER_NUMBER=2600000
export WARMUP_ORDER_NUMBER=2400000
export SLEEP_TIME_MS=70000

sum_throughput=0
throughputs=()

for run in $(seq 1 $N); do
  echo "=== Test run $run of $N ==="

  # ─── start infra and services ─────────────────────────────────────────────
  ./startInfra.sh

  docker compose up -d exchange-1
  sleep 2
  docker compose up -d exchange-2
  sleep 10
  docker compose up -d trader-1
  sleep 10

  # ─── warmup verification
  docker logs trader-1 | grep --color=always '|11=100|'; date; echo; sleep 110

  docker stats --no-stream; date; echo

  # ─── capture relevant log lines once ─────────────────────────────────────
  LAST_WARMUP_ORDER_ID=$((WARMUP_ORDER_NUMBER - 1))
  KEY="\\|11=($LAST_WARMUP_ORDER_ID|$WARMUP_ORDER_NUMBER|$TOTAL_ORDER_NUMBER)\\|"
  LOGS=$(docker logs trader-1 2>&1 | grep -E "$KEY")

  # (optional) highlight captured lines for inspection
  echo "$LOGS" | grep -E --color=always "$KEY"
  date
  echo

  # ─── extract the timestamps ────────────────────────────────────────────────
  FIRST_NEW_ORDER_TIME=$(
    echo "$LOGS" \
      | grep "OUT:.*35=D.*|11=$WARMUP_ORDER_NUMBER|" \
      | head -1 \
      | awk '{print $1}'
  )
  echo "First new order time: [$FIRST_NEW_ORDER_TIME]"

  LAST_CANCEL_ORDER_TIME=$(
    echo "$LOGS" \
      | grep "IN:.*35=8.*|11=$TOTAL_ORDER_NUMBER|.*39=4" \
      | tail -1 \
      | awk '{print $1}'
  )
  echo "Last cancel order time: [$LAST_CANCEL_ORDER_TIME]"

  if [[ -z $FIRST_NEW_ORDER_TIME || -z $LAST_CANCEL_ORDER_TIME ]]; then
    echo "‼️  Could not locate timestamps – aborting run $run"
    exit 1
  fi

  # ─── convert HH:MM:SS.mmm → seconds since midnight ─────────────────────────
  first_sec=$(echo "$FIRST_NEW_ORDER_TIME" \
    | awk -F '[:.]' '{print ($1*3600 + $2*60 + $3) + ($4/1000)}'
  )
  last_sec=$(echo "$LAST_CANCEL_ORDER_TIME" \
    | awk -F '[:.]' '{print ($1*3600 + $2*60 + $3) + ($4/1000)}'
  )

  duration=$(echo "$last_sec - $first_sec" | bc -l)
  total_requests=$(echo "($TOTAL_ORDER_NUMBER - $WARMUP_ORDER_NUMBER)*1.5" | bc -l)
  throughput=$(echo "scale=2; $total_requests / $duration" | bc -l)

  echo
  echo "First new‑order at:  $FIRST_NEW_ORDER_TIME  → ${first_sec}s"
  echo "Last cancel     at:  $LAST_CANCEL_ORDER_TIME  → ${last_sec}s"
  echo "Elapsed:             ${duration}s"
  echo "Request count:       ${total_requests}"
  echo "Run $run throughput: ${throughput} req/sec"
  echo

  sum_throughput=$(echo "$sum_throughput + $throughput" | bc -l)
  throughputs+=("$throughput")

  echo "────────────────────────────────────────────────────────────────────────────"
  echo
done

# ─── compute and print average and data points ───────────────────────────────
avg_throughput=$(echo "scale=2; $sum_throughput / $N" | bc -l)
echo "=== All $N runs complete ==="
echo "Throughputs         : ${throughputs[*]}"
echo "Average throughput  : ${avg_throughput} req/sec"
