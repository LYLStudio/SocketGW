#!/bin/bash
# ============================================================
# SocketGW Benchmark - Optimized Combo Finder
# Tests: Direct vs Via-Gateway × Backend count (1~5)
# Each test: 8s duration, 200 clients BASIC + 100 clients ADVANCED
# ============================================================

set -euo pipefail

WORK_DIR="/home/dev/AI/SocketGW"
TEST_DURATION=8
BASIC_CLIENTS=200
ADV_CLIENTS=100
GATEWAY_PORT=8080
BE_START=5001
RESULTS_DIR="/tmp/bench_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

kill_ports() {
    for p in $(seq $BE_START $((BE_START+10))) $GATEWAY_PORT $((GATEWAY_PORT+1)); do
        fuser -k "${p}/tcp" 2>/dev/null || true
    done
    sleep 2
}

start_backends() {
    local n=$1
    for i in $(seq 0 $((n-1))); do
        local port=$((BE_START+i))
        SOCKET_SERVER_PORT=$port dotnet run --project "$WORK_DIR/SocketServer" \
            > "$RESULTS_DIR/be_${port}.log" 2>&1 &
    done
    sleep 4
    for i in $(seq 0 $((n-1))); do
        local port=$((BE_START+i))
        fuser "${port}/tcp" >/dev/null 2>&1 || return 1
    done
    echo -e "${GREEN}✓${NC} ${n} backends on ports $BE_START..$((BE_START+n-1))"
}

start_gateway() {
    dotnet run --project "$WORK_DIR/GatewayApp" > "$RESULTS_DIR/gw.log" 2>&1 &
    sleep 4
    if fuser "${GATEWAY_PORT}/tcp" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Gateway on port $GATEWAY_PORT"
        return 0
    else
        echo -e "${RED}✗${NC} Gateway failed!"
        tail -5 "$RESULTS_DIR/gw.log"
        return 1
    fi
}

parse_metric() { local file=$1; grep -oP "$2" "$file" | head -1 || echo "0"; }

run_test() {
    local mode=$1 gw_mode=$2 be_count=$3 test_type=$4 clients=$5 target_port=$6
    local label="${gw_mode}_be${be_count}_${test_type}"
    local outfile="$RESULTS_DIR/${label}.txt"

    if [ "$test_type" = "basic" ]; then
        dotnet run --project "$WORK_DIR/SocketTests" -- basic \
            --clients "$clients" --duration "$TEST_DURATION" --port "$target_port" \
            > "$outfile" 2>&1 || true
    else
        dotnet run --project "$WORK_DIR/SocketTests" -- advanced \
            --clients "$clients" --duration "$TEST_DURATION" --port "$target_port" \
            --batch $((clients/4)) \
            > "$outfile" 2>&1 || true
    fi

    local pf=$(parse_metric "$outfile" '\[(PASS|FAIL)\]')
    local conn=$(parse_metric "$outfile" '[0-9]+/[0-9]+ connected')
    local avgLat=$(parse_metric "$outfile" '(Avg.*?:\s*\K[0-9.]+)')
    local msgRate=$(parse_metric "$outfile" '(Msg Rate.*?:\s*\K[0-9.]+)')
    local tput=$(parse_metric "$outfile" '(Throughput.*?:\s*\K[0-9.]+)')
    local errs=$(parse_metric "$outfile" '(Errors.*?:\s*\K[0-9]+)' | tail -1)

    echo "${mode}|${gw_mode}|${be_count}|${test_type}|${pf}|${conn}|${avgLat}|${msgRate}|${tput}|${errs}"
}

# ============================================================
echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════╗"
echo "║  SocketGW Benchmark — Gateway + Backend Combo Finder ║"
echo "╚══════════════════════════════════════════════════════╝${NC}"
echo ""

RESULTS_CSV="$RESULTS_DIR/results.csv"
echo "" > "$RESULTS_CSV"

TEST_NUM=0
for gw_mode in direct via-gateway; do
    for be_count in 1 2 3 4 5; do

        TEST_NUM=$((TEST_NUM+1))
        echo -e "${BOLD}--- Test #${TEST_NUM}: ${gw_mode} × ${be_count} backend(s) ---${NC}"
        kill_ports

        if ! start_backends "$be_count"; then
            echo -e "${RED}✗ Backend failed, skipping${NC}"
            continue
        fi

        if [ "$gw_mode" = "via-gateway" ]; then
            if ! start_gateway; then
                kill_ports; continue
            fi
            target_port=$GATEWAY_PORT
        else
            target_port=$BE_START  # direct to first backend only
        fi

        echo -n "  BASIC...   "
        basic_line=$(run_test "$TEST_NUM" "$gw_mode" "$be_count" "basic" "$BASIC_CLIENTS" "$target_port")
        echo "  $basic_line" | tee -a "$RESULTS_CSV"

        echo -n "  ADVANCED.. "
        adv_line=$(run_test "$TEST_NUM" "$gw_mode" "$be_count" "advanced" "$ADV_CLIENTS" "$target_port")
        echo "  $adv_line" | tee -a "$RESULTS_CSV"

        echo -e "${GREEN}✓ Test #${TEST_NUM} done${NC}"
    done
done

kill_ports
echo ""

# ============================================================
# Report
# ============================================================
echo -e "${BOLD}${CYAN}══════════════════════════════════════════════${NC}"
echo -e "${BOLD}              BENCHMARK REPORT${NC}"
echo -e "${BOLD}${CYAN}══════════════════════════════════════════════${NC}"
echo ""

# Header for readable output
printf "%-14s %-8s %-10s %-6s %-12s %s\n" \
    "Combo" "Backend" "Type" "Pass?" "Throughput" "MsgRate" 
echo "----------------------------------------------------------------------"

# Show all results sorted
grep -v '^$' "$RESULTS_CSV" | while IFS='|' read num gw be tp pf conn avg mr tput err; do
    marker=""
    [ "$pf" = "[PASS]" ] && marker="${GREEN}✓${NC}" || marker="${RED}✗${NC}"
    printf "%-14s %-8s %-10s ${marker}%5s  %s MB/s   %s msg/s  err=%s\n" \
        "${gw}" "${be}" "${tp}" "$pf" "${tput}" "${mr}" "${err}"
done

echo ""
echo -e "${BOLD}=== Best BASIC Throughput (Top 3) ===${NC}"
grep '|basic|' "$RESULTS_CSV" | grep '\[PASS\]' | sort -t'|' -k9 -rn | head -3 | \
    while IFS='|' read num gw be tp pf conn avg mr tput err; do
        echo "  ${GREEN}#${num}${NC} | ${gw} × BE:${be} | ${tput} MB/s | ${mr} msg/s"
    done

echo ""
echo -e "${BOLD}=== Best ADVANCED Throughput (Top 3) ===${NC}"
grep '|advanced|' "$RESULTS_CSV" | grep '\[PASS\]' | sort -t'|' -k9 -rn | head -3 | \
    while IFS='|' read num gw be tp pf conn avg mr tput err; do
        echo "  ${GREEN}#${num}${NC} | ${gw} × BE:${be} | ${tput} MB/s | ${mr} msg/s"
    done

echo ""
echo -e "${BOLD}=== Lowest AVG Latency (Advanced, Top 3) ===${NC}"
grep '|advanced|' "$RESULTS_CSV" | grep '\[PASS\]' | sort -t'|' -k7 -n | head -3 | \
    while IFS='|' read num gw be tp pf conn avg mr tput err; do
        echo "  ${GREEN}#${num}${NC} | ${gw} × BE:${be} | AvgLat: ${avg}ms | ${tput} MB/s"
    done

echo ""
echo -e "${BOLD}Raw data saved to: ${RESULTS_DIR}/${NC}"
ls "$RESULTS_DIR"/*.txt 2>/dev/null | head -5
echo "..."