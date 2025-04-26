#!/usr/bin/env bash

# ----------------------------------------------------------------------------------------------------
# Failover test evaluates how quickly the trading system recovers from an Exchange leader failure.

# Test Scenario:
#   1. All core components are launched: Aeron, ZooKeeper, Exchange nodes, and Trader.
#   2. Trader enters an infinite trading loop:
#       2.1. Trader submits a New Order to the Exchange.
#       2.2. Upon receiving a New Order Ack from the Exchange:
#            2.2.1. Trader sends a Cancel Order for the same ClOrdID.
#       2.3. Upon receiving a Cancel Order Ack from the Exchange:
#            2.3.1. Trader sends the next New Order.
#       2.4. Steps 2.2–2.3 repeat indefinitely until the test is stopped.
#   3. The leader Exchange (exchange-1) is paused to simulate failure.
#   4. Trader begins periodically resending the request (New or Cancel) due to timeout while waiting for an Ack.
#   5. Once a follower Exchange (exchange-2 or exchange-3) becomes the new leader, it responds to the resent request.
#   6. Failover time is measured as:
#        (Time of Resend Success) − (Timestamp of Original Request)

# Notes:
#   - There can be multiple variations of this scenario depending on the timing of the failure:
#     - The leader Exchange may fail after the New or Cancel Order is sent.
#     - It may or may not replicate the order to a follower before failing.
#     - It may or may not respond to the Trader before failing.
#   - Regardless of the failure moment, the Exchange guarantees that once a request is acknowledged,
#     it will not be lost, even if the leader fails immediately after.
#   - The test runs N cycles; results are collected and averaged.

# Outputs:
#   - Per-run timestamps: original request, resend init, and resend success.
#   - Per-run failover durations.
#   - A summary of all failover durations and the average.
# ----------------------------------------------------------------------------------------------------

set -e

N=${1:-5}               # how many fail‑over cycles to execute
export TEST_ID=5        # kept from your original script

now() { date '+%Y-%m-%d %H:%M:%S'; }

# Accumulators                                                                #
failovers=()            # array of per‑run fail‑over times
sum_failover_time=0     # running sum for average

for run in $(seq 1 $N); do
  echo
  echo "══════════════  Cycle $run / $N  ══════════════"
  echo

  # ─── bring up infra ───────────────────────────────────────────────────────
  ./startInfra.sh
  echo "$(now)  [run $run]  Started Aeron + ZooKeeper";         sleep 2

  docker compose up -d exchange-1
  echo "$(now)  [run $run]  Started exchange-1";                sleep 2

  docker compose up -d exchange-2 exchange-3
  echo "$(now)  [run $run]  Started exchange-2 & 3";            sleep 9

  docker compose up -d trader-1
  echo "$(now)  [run $run]  Started trader-1";                  sleep 15

  # ─── simulate leader failure ──────────────────────────────────────────────
  docker compose pause exchange-1
  echo "$(now)  [run $run]  Paused exchange-1 (leader)";        sleep 15
  echo

  # Extract ClOrdID of the re-sent order request
  ORDER_ID=$(docker compose logs trader-1 \
             | grep 'First Resend init' \
             | grep -oE '\|11=[0-9]+\|' \
             | head -1 | cut -d'=' -f2 | tr -d '|')

  KEY="|11=$ORDER_ID|\|LEADER\|FOLLOWER"
  ADD_COLOR_KEY="ACCEPTED\|REQUEST_REJECT\|CANCELED\|CANCEL_REJECTED\|35=D\|35=F\|First Resend succeeded\|First Resend init\|"

  echo "[run $run]  Extracted ORDER_ID: $ORDER_ID"
  echo "------------------------------------------------------------------------------"
  docker compose logs trader-1 | grep --color=always "$KEY" | grep --color=always "$ADD_COLOR_KEY"
  echo "------------------------------------------------------------------------------"

  for ex in exchange-1 exchange-2 exchange-3; do
    echo "[run $run]  Logs from $ex"
    docker compose logs "$ex" | grep --color=always "$KEY" | grep --color=always "$ADD_COLOR_KEY"
    echo "------------------------------------------------------------------------------"
  done

  # ⏱  Compute fail‑over time                                                #
  TRADER_LOG=$(docker compose logs trader-1 2>&1)

  # first “First Resend init”
  init_line_num=$(echo "$TRADER_LOG" | grep -n 'First Resend init' | head -1 | cut -d: -f1)
  [[ -z $init_line_num ]] && { echo "‼️  [run $run] No 'First Resend init' found"; exit 1; }

  # original Order Request (New |35=D| or Cancel |35=F|) before Resend
  orig_line=$(echo "$TRADER_LOG" \
              | head -n $((init_line_num-1)) \
              | grep "|11=$ORDER_ID|" \
              | grep "OUT:" \
              | grep -E "35=D|35=F" \
              | tail -1)

  ORIG_TS=$(echo "$orig_line"           | awk '{print $3}')
  INIT_TS=$(echo "$TRADER_LOG"          | grep 'First Resend init'      | head -1 | awk '{print $3}')
  SUCC_TS=$(echo "$TRADER_LOG"          | grep 'First Resend succeeded' | head -1 | awk '{print $3}')

  if [[ -z $ORIG_TS || -z $SUCC_TS ]]; then
    echo "‼️  [run $run] Could not find original or success timestamps"
    exit 1
  fi

  # HH:MM:SS.mmm → seconds since midnight
  to_secs() { awk -F '[:.]' '{print ($1*3600)+($2*60)+$3 + $4/1000}'; }
  orig_sec=$(echo "$ORIG_TS" | to_secs)
  succ_sec=$(echo "$SUCC_TS" | to_secs)

  failover=$(echo "$succ_sec - $orig_sec" | bc -l)

  echo "[run $run]  Original request     : $ORIG_TS"
  echo "[run $run]  First Resend init    : $INIT_TS"
  echo "[run $run]  First Resend success : $SUCC_TS"
  echo "[run $run]  Fail‑over time       : ${failover} s"
  echo

  failovers+=("$failover")
  sum_failover_time=$(echo "$sum_failover_time + $failover" | bc -l)
done

# Summary
avg_fail=$(echo "scale=3; $sum_failover_time / ${#failovers[@]}" | bc -l)

echo "══════════════  Summary of $N runs  ══════════════"
echo "Fail‑over times     : $(printf '%s, ' "${failovers[@]}" | sed 's/, $//')"
echo "Average fail‑over   : ${avg_fail} s"

#docker compose down
