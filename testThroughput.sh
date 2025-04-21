#!/usr/bin/env bash

# ----------------------------------------------------------------------------------------------------
# Performance test evaluates the sustained throughput of the trading system under load.

# Test Scenario:
#   1. All core components are launched: Aeron, ZooKeeper, Exchange nodes, and Trader.
#   2. Warm-up phase (to trigger Java JIT compilation):
#       2.1. Trader sends a New Sell Qty=100 Order to the Exchange.
#       2.2. Trader sends a New Buy Qty=30 Order to the Exchange.
#       2.3. Trader sends a Cancel for the remaining Sell Order qty.
#       2.4. Steps 2.1–2.3 are repeated until WARMUP_ORDER_NUMBER is reached.
#   3. Wait for SLEEP_TIME_MS milliseconds to allow the Exchange to process all warm-up orders.
#   4. Performance Test phase:
#       4.1. Trader sends a New Sell Qty=100 Order to the Exchange.
#       4.2. Trader sends a New Buy Qty=30 Order to the Exchange.
#       4.3. Trader sends a Cancel for the Sell Order.
#       4.4. Steps 4.1–4.3 are repeated until TOTAL_ORDER_NUMBER is reached.

# Notes:
#   - Trader does not wait for a response from the Exchange before sending the next order request.
#   - This ensures the Trader sends requests as fast as possible, stressing the Exchange to keep up.

# Metrics:
#   - Total Orders Sent              : TOTAL_ORDER_NUMBER
#   - Orders After Warm-up           : TOTAL_ORDER_NUMBER - WARMUP_ORDER_NUMBER
#   - Sell Orders                    : 50% of total order count
#   - Buy Orders                     : 50% of total order count
#   - Sell Order Requests            : 2 × Sell Orders (1 New + 1 Cancel)
#   - Buy Order Requests             : 1 × Buy Orders (1 New only)
#   - Total Order Requests           : (TOTAL_ORDER_NUMBER - WARMUP_ORDER_NUMBER) × 1.5
#   - Throughput                     : Total Order Requests / Time interval (between first New Order and last Cancel Ack)
#   - Output includes per-run throughput and the average across N test runs.
# ----------------------------------------------------------------------------------------------------

N=5 # Number of test cycles
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
echo "Throughputs         : $(printf '%s, ' "${throughputs[@]}" | sed 's/, $//')"
echo "Average throughput  : ${avg_throughput} req/sec"

docker compose down