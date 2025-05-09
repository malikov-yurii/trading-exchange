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

N=${1:-1}               # how many test cycles to execute
export TEST_ID=4
export FIX_LOGGER=TAGGED
export TOTAL_ORDER_NUMBER=2600000
export WARMUP_ORDER_NUMBER=2400000
export SLEEP_TIME_MS=70000

sum_throughput=0
throughputs=()

sum_in_data_rate=0
in_data_rates=()

sum_out_data_rate=0
out_data_rates=()

for run in $(seq 1 $N); do
  echo "======================= Test run $run of $N ======================="

  # ─── start infra and services ─────────────────────────────────────────────
  ./startInfra.sh

  docker compose up -d exchange-1
  sleep 2
  docker compose up -d exchange-2
  sleep 10
  docker compose up -d trader-1
  sleep 10

  # ─── warmup verification
  docker logs trader-1 | grep --color=always '|11=100|'; date; echo; sleep 125

  docker stats --no-stream; date; echo

  # ─── capture relevant log lines once ─────────────────────────────────────
  LAST_WARMUP_ORDER_ID=$((WARMUP_ORDER_NUMBER - 1))
  KEY="\\|11=($LAST_WARMUP_ORDER_ID|$WARMUP_ORDER_NUMBER|$TOTAL_ORDER_NUMBER)\\|"
  LOGS=$(docker logs trader-1 2>&1 | grep -E "$KEY")

  # (optional) highlight captured lines for inspection
  echo "$LOGS" | grep -E --color=always "$KEY"
  date
  echo

  # ─── extract the timestamps and byte offsets ────────────────────────────────────────────────
  FIRST_NEW_ORDER_LOG_LINE=$( echo "$LOGS"  | grep "|11=$WARMUP_ORDER_NUMBER|" | grep '|35=D|' | head -1 )
  FIRST_NEW_ORDER_TIME=$( echo "$FIRST_NEW_ORDER_LOG_LINE" | awk '{print $1}' )
  echo "First new order time: [$FIRST_NEW_ORDER_TIME]"
  FIRST_NEW_ORDER_BYTE_OFFSET=$( echo "$FIRST_NEW_ORDER_LOG_LINE" | awk '{print $4}' )
  echo "First new order byte offset (OUT stream): [$FIRST_NEW_ORDER_BYTE_OFFSET]"

  LAST_CANCEL_ORDER_LOG_LINE=$( echo "$LOGS"  | grep "|11=$TOTAL_ORDER_NUMBER|" | grep '|35=F|' | head -1 )
  LAST_CANCEL_ORDER_TIME=$( echo "$LAST_CANCEL_ORDER_LOG_LINE" | awk '{print $1}' )
  echo "Last cancel order time: [$LAST_CANCEL_ORDER_TIME]"
  LAST_CANCEL_ORDER_BYTE_OFFSET=$( echo "$LAST_CANCEL_ORDER_LOG_LINE" | awk '{print $4}' )
  echo "Last cancel order byte offset (OUT stream): [$LAST_CANCEL_ORDER_BYTE_OFFSET]"

  FIRST_NEW_ORDER_ACK_LOG_LINE=$( echo "$LOGS"  | grep "|11=$WARMUP_ORDER_NUMBER|" | grep '|39=0|' | head -1 )
  FIRST_NEW_ORDER_ACK_TIME=$( echo "$FIRST_NEW_ORDER_ACK_LOG_LINE" | awk '{print $1}' )
  echo "First new order ack time: [$FIRST_NEW_ORDER_ACK_TIME]"
  FIRST_NEW_ORDER_ACK_BYTE_OFFSET=$( echo "$FIRST_NEW_ORDER_ACK_LOG_LINE" | awk '{print $5}' )
  echo "First new order ack byte offset (IN stream): [$FIRST_NEW_ORDER_ACK_BYTE_OFFSET]"

  LAST_CANCEL_ORDER_ACK_LOG_LINE=$( echo "$LOGS"  | grep "|11=$TOTAL_ORDER_NUMBER|" | grep '|39=4|' | tail -1 )
  LAST_CANCEL_ORDER_ACK_TIME=$( echo "$LAST_CANCEL_ORDER_ACK_LOG_LINE" | awk '{print $1}' )
  echo "Last cancel order ack time: [$LAST_CANCEL_ORDER_ACK_TIME]"
  LAST_CANCEL_ORDER_ACK_BYTE_OFFSET=$( echo "$LAST_CANCEL_ORDER_ACK_LOG_LINE" | awk '{print $5}' )
  echo "Last cancel order ack byte offset (IN stream): [$LAST_CANCEL_ORDER_ACK_BYTE_OFFSET]"

  if [[ -z $FIRST_NEW_ORDER_TIME || -z $LAST_CANCEL_ORDER_TIME ]]; then
    echo "‼️  Could not locate timestamps – aborting run $run"
    exit 1
  fi

  # ─── convert HH:MM:SS.mmm → seconds since midnight ─────────────────────────
  to_ms() {
    local time_str=$1
    awk -F '[:.]' -v t="$time_str" 'BEGIN {
      split(t, parts, /[:.]/)
      print (parts[1]*3600000 + parts[2]*60000 + parts[3]*1000 + parts[4])
    }'
  }

  first_new_order_ms=$(to_ms "$FIRST_NEW_ORDER_TIME")
  last_cancel_order_ms=$(to_ms "$LAST_CANCEL_ORDER_TIME")

  first_new_order_ack_ms=$(to_ms "$FIRST_NEW_ORDER_ACK_TIME")
  last_cancel_order_ack_ms=$(to_ms "$LAST_CANCEL_ORDER_ACK_TIME")

  total_requests=$(echo "($TOTAL_ORDER_NUMBER - $WARMUP_ORDER_NUMBER)*1.5" | bc -l)

  out_bytes=$(echo "$LAST_CANCEL_ORDER_BYTE_OFFSET - $FIRST_NEW_ORDER_BYTE_OFFSET" | bc -l)
  in_bytes=$(echo "$LAST_CANCEL_ORDER_ACK_BYTE_OFFSET - $FIRST_NEW_ORDER_ACK_BYTE_OFFSET" | bc -l)

  echo
  echo "Trader OUT Stream"
  echo "First new order     :  $FIRST_NEW_ORDER_TIME"
  echo "Last cancel         :  $LAST_CANCEL_ORDER_TIME"
  elapsed_out_s=$(echo "scale=3; ($last_cancel_order_ms - $first_new_order_ms) / 1000" | bc)
  echo "Elapsed             :  ${elapsed_out_s}s"
  echo "Sent                :  $(echo "scale=1; $out_bytes / 1000000" | bc)MB"
  out_data_rate=$(echo "scale=1; $out_bytes / $elapsed_out_s / 1000000" | bc)
  echo "Sent Data Rate      :  ${out_data_rate}MB/s"

  echo
  echo "Trader IN Stream"
  echo "First new order ACK :  $FIRST_NEW_ORDER_ACK_TIME"
  echo "Last cancel ACK     :  $LAST_CANCEL_ORDER_ACK_TIME"
  elapsed_in_s=$(echo "scale=1; ($last_cancel_order_ack_ms - $first_new_order_ack_ms) / 1000" | bc)
  echo "Elapsed             :  ${elapsed_in_s}s"
  echo "Received            :  $(echo "scale=1; $in_bytes / 1000000" | bc)MB"
  in_data_rate=$(echo "scale=1; $in_bytes / $elapsed_in_s / 1000000" | bc)
  echo "Received Data Rate  :  ${in_data_rate}MB/s"

  echo
  echo "Request count                     :  ${total_requests}"
  elapsed=$(echo "scale=3; ($last_cancel_order_ack_ms - $first_new_order_ms) / 1000" | bc)
  throughput=$(echo "scale=0; $total_requests / $elapsed" | bc -l)
  echo "Elapsed [first request, last ack] :  ${elapsed}s"
  echo "Run $run Throughput                  :  ${throughput} req/sec"
  echo

  sum_throughput=$(echo "$sum_throughput + $throughput" | bc -l)
  throughputs+=("$throughput")

  sum_out_data_rate=$(echo "$sum_out_data_rate + $out_data_rate" | bc -l)
  out_data_rates+=("$out_data_rate")

  sum_in_data_rate=$(echo "$sum_in_data_rate + $in_data_rate" | bc -l)
  in_data_rates+=("$in_data_rate")
  echo
done

# ─── compute and print average and data points ───────────────────────────────
avg_throughput=$(echo "scale=2; $sum_throughput / $N" | bc -l)
avg_out_data_rate=$(echo "scale=2; $sum_out_data_rate / $N" | bc -l)
avg_in_data_rate=$(echo "scale=2; $sum_in_data_rate / $N" | bc -l)

echo "============================== All $N test runs complete =============================="
echo
echo "Throughputs           : $(printf '%s, ' "${throughputs[@]}" | sed 's/, $//')"
echo "Average Throughput    : ${avg_throughput} req/sec"
echo "Trader OUT Data Rates : $(printf '%s, ' "${out_data_rates[@]}" | sed 's/, $//')"
echo "Average OUT Data Rate : ${avg_out_data_rate} MB/s"
echo "Trader IN Data Rates  : $(printf '%s, ' "${in_data_rates[@]}" | sed 's/, $//')"
echo "Average IN Data Rate  : ${avg_in_data_rate} MB/s"

docker compose stop