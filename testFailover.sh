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

N=${1:-1}               # how many test cycles to execute
export TEST_ID=5        # kept from your original script
export FIX_LOGGER=TAGGED
export TRADER_JAVA_TOOL_OPTIONS="-Dlogback.configurationFile=/root/jar/logback-debug.xml -XX:+TieredCompilation -XX:TieredStopAtLevel=4 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:+UseStringDeduplication -Xms2G -Xmx4G"
export EXCHANGE_JAVA_TOOL_OPTIONS="-Dlogback.configurationFile=/root/jar/logback-debug.xml -XX:+TieredCompilation -XX:TieredStopAtLevel=4 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:+UseStringDeduplication -Xms2G -Xmx4G"

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

  # Grab the last “Failover succeeded” line
    log_line=$(docker compose logs trader-1 2>&1 \
      | grep 'Failover succeeded in' \
      | tail -1)

  if [[ -z $log_line ]]; then
    echo "‼️  [run $run] No 'Failover succeeded' line found"; exit 1
  fi

  # Timestamp at the start of the line (HH:MM:SS.mmm)
  SUCC_TS=$(echo "$log_line" | awk '{print $3}')

  # Extract ORDER_ID via ERE + sed
  ORDER_ID=$(echo "$log_line" \
    | grep -oE '\|11=[0-9]+' \
    | sed 's/|11=//')

  # Fail‑over in milliseconds inside “…in [1234] ms”
  FAILOVER_MS=$(echo "$log_line" | grep -oP 'succeeded in \[\K[0-9]+')

  # newOrderTime / cancelOrderTime / resendingTime fields
  new_ts=$(echo "$log_line"      | grep -oP 'newOrderTime:\K[^,]*')
  cancel_ts=$(echo "$log_line"   | grep -oP 'cancelOrderTime:\K[^,]*')
  init_ts=$(echo "$log_line"     | grep -oP 'resendingTime:\K[^ ]*')

  # choose original‑request timestamp
  if [[ -n "$cancel_ts" && "$cancel_ts" != "null" ]]; then
      ORIG_TS=$cancel_ts   # we only need the HH:MM:SS part
  else
      ORIG_TS=$new_ts
  fi
  ORIG_TS=${ORIG_TS#*T}           # strip the date part -> HH:MM:SS.nnnnnnnnn
  INIT_TS=${init_ts#*T}

  # convert FAILOVER_MS → seconds
  failover=$(awk "BEGIN {printf \"%.3f\", $FAILOVER_MS/1000}")

  failovers+=("$failover")
  sum_failover_time=$(awk "BEGIN {print $sum_failover_time + $failover}")

  KEY="|11=$ORDER_ID|\|LEADER\|FOLLOWER"
  ADD_COLOR_KEY="ACCEPTED\|REQUEST_REJECT\|CANCELED\|CANCEL_REJECTED\|35=D\|35=F\|First Resend succeeded\|First Resend init\|"

  echo "------------------------------------------------------------------------------"
  docker compose logs trader-1 | grep --color=always "$KEY" | grep --color=always "$ADD_COLOR_KEY" | grep -v 'incorrect field\|DEBUG'
  echo "------------------------------------------------------------------------------"

  for ex in exchange-1 exchange-2 exchange-3; do
    echo "[run $run]  Logs from $ex"
    docker compose logs "$ex" | grep --color=always "$KEY" | grep --color=always "$ADD_COLOR_KEY" | grep -v 'incorrect field\|DEBUG'
    echo "------------------------------------------------------------------------------"
  done

  echo "[run $run]  ORDER_ID          : $ORDER_ID"
  echo "[run $run]  Original request  : $ORIG_TS"
  echo "[run $run]  Last Resend       : $INIT_TS"
  echo "[run $run]  Resend success    : $SUCC_TS"
  echo "[run $run]  Fail‑over time    : ${failover} s"
  echo

done

# Summary
avg_fail=$(echo "scale=3; $sum_failover_time / ${#failovers[@]}" | bc -l)

echo "══════════════  Summary of $N runs  ══════════════"
echo "Fail‑over times     : $(printf '%s, ' "${failovers[@]}" | sed 's/, $//')"
echo "Average fail‑over   : ${avg_fail} s"

docker compose stop
