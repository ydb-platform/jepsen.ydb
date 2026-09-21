#!/usr/bin/env bash

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 --nodes-file <path> --db-name <name>

  --nodes-file  Path to file with cluster node list
  --db-name     YDB database name
EOF
    exit 1
}

NODES_FILE=""
DB=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes-file) NODES_FILE="$2"; shift 2 ;;
        --db-name)    DB="$2";         shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[[ -z "$NODES_FILE" ]] && { echo "Error: --nodes-file is required"; usage; }
[[ -z "$DB" ]]         && { echo "Error: --db-name is required";    usage; }

REPORT_FILE="jepsen-report-$(date +%Y%m%d-%H%M%S).md"

urlencode() {
    python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$1"
}

append_result() {
    local name="$1"
    local exit_code="$2"
    local latest
    latest=$(readlink store/latest 2>/dev/null || true)

    local status
    if [[ "$exit_code" -eq 0 ]]; then
        status="success"
    else
        status="FAILED (exit $exit_code)"
    fi

    local link="N/A"
    if [[ -n "$latest" ]]; then
        link="[report](http://$(hostname):9000/files/$(urlencode "$latest"))"
    fi

    echo "| $name | $status | $link |" >> "$REPORT_FILE"
}

run_test() {
    local name="$1"; shift
    echo ""
    echo "=== $name ==="
    local exit_code=0
    lein run test "$@" || exit_code=$?
    append_result "$name" "$exit_code"
}

# ── Report header ─────────────────────────────────────────────────────────────
{
    echo "# Jepsen Test Report"
    echo ""
    echo "- **Date:** $(date)"
    echo "- **Nodes file:** \`$NODES_FILE\`"
    echo "- **Database:** \`$DB\`"
    echo ""
    echo "| Test | Status | Report |"
    echo "|------|--------|--------|"
} > "$REPORT_FILE"

# ── Common args ───────────────────────────────────────────────────────────────
COMMON=(
    --nodes-file "$NODES_FILE"
    --db-name "$DB"
    --no-ssh
    --concurrency 10n
    --key-count 15
    --max-writes-per-key 16
    --max-txn-length 4
    --time-limit 600
    --rate 1500
    --ballast-size 10000
    --nemesis all
    --nemesis-attack-duration 5
    --nemesis-rest-duration 60
)

# ── 6 test configurations ─────────────────────────────────────────────────────

run_test "ydb-serializable (row)" \
    "${COMMON[@]}" \
    --model ydb-serializable \
    --with-opindex \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type row

run_test "ydb-serializable (column)" \
    "${COMMON[@]}" \
    --model ydb-serializable \
    --with-opindex \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type column

run_test "snapshot-isolation (row)" \
    "${COMMON[@]}" \
    --model snapshot-isolation \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type row

run_test "snapshot-isolation (column)" \
    "${COMMON[@]}" \
    --model snapshot-isolation \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type column

run_test "read-committed (row)" \
    "${COMMON[@]}" \
    --model read-committed \
    --workload-name append-single-row \
    --store-type row

run_test "read-committed (column)" \
    "${COMMON[@]}" \
    --model read-committed \
    --workload-name append-single-row \
    --store-type column

echo ""
echo "Report: $REPORT_FILE"
