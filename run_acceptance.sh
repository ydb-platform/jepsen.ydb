#!/usr/bin/env bash

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 --nodes-file <path> --db-name <name> [--isolation <model>]

  --nodes-file  Path to file with cluster node list
  --db-name     YDB database name
  --isolation   Run only the row and column scenarios for this model
EOF
    exit 1
}

NODES_FILE=""
DB=""
ISOLATION=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes-file) NODES_FILE="$2"; shift 2 ;;
        --isolation)  ISOLATION="$2"; shift 2 ;;
        --db-name)    DB="$2";         shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

[[ -z "$NODES_FILE" ]] && { echo "Error: --nodes-file is required"; usage; }
[[ -z "$DB" ]]         && { echo "Error: --db-name is required";    usage; }

case "$ISOLATION" in
    ""|ydb-serializable|snapshot-isolation|read-committed) ;;
    *) usage ;;
esac

failed=0
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# Reuse the caller's SSH setup unless a RoboSSH token file is supplied.
if [[ -n "${ROBOSSH_TOKEN_FILE:-}" ]]; then
    robossh_agent="${ROBOSSH_AGENT:-robossh_agent}"
    if [[ ! -r "$ROBOSSH_TOKEN_FILE" ]]; then
        printf 'Cannot read RoboSSH token file: %s\n' "$ROBOSSH_TOKEN_FILE" >&2
        exit 1
    fi
    robossh_log=$(mktemp "${TMPDIR:-/tmp}/jepsen-robossh.XXXXXX") || exit 1
    if ! agent_setup=$(ROBOSSH_TOKEN=$(<"$ROBOSSH_TOKEN_FILE") \
        "$robossh_agent" --with-certs --request-ca secure 2>"$robossh_log"); then
        printf 'RoboSSH setup failed. See %s\n' "$robossh_log" >&2
        exit 1
    fi
    unset SSH_AGENT_PID SSH_AUTH_SOCK
    eval "$agent_setup" >/dev/null
    if [[ -z "${SSH_AGENT_PID:-}" ]]; then
        printf 'RoboSSH did not return an agent PID. See %s\n' "$robossh_log" >&2
        exit 1
    fi
    trap 'kill "$SSH_AGENT_PID" 2>/dev/null || true' EXIT

    if ! JEPSEN_SSH_USERNAME=$(ssh-add -L | ssh-keygen -L -f - |
        awk '/Principals:/{getline; if (!seen++ && NF) print $1}'); then
        printf 'Cannot read the RoboSSH certificate principal.\n' >&2
        exit 1
    fi
    if [[ -z "$JEPSEN_SSH_USERNAME" || "$JEPSEN_SSH_USERNAME" == "(none)" ]]; then
        printf 'RoboSSH certificate has no SSH principal.\n' >&2
        exit 1
    fi
    export JEPSEN_SSH_USERNAME
fi

REPORT_FILE="jepsen-report-$(date +%Y%m%d-%H%M%S).md"

urlencode() {
    python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$1"
}

append_result() {
    local name="$1"
    local exit_code="$2"
    local latest="$3"

    local status
    if [[ "$exit_code" -eq 0 ]]; then
        status="success"
    else
        status="FAILED (exit $exit_code)"
    fi

    local link="N/A"
    if [[ -n "$latest" ]]; then
        link="[report](store/$(urlencode "$latest")/static-report.html)"
    fi

    echo "| $name | $status | $link |" >> "$REPORT_FILE"
}

run_test() {
    local model="$1" name="$2"; shift 2
    [[ -z "$ISOLATION" || "$ISOLATION" == "$model" ]] || return 0
    echo ""
    echo "=== $name ==="
    local previous latest
    previous=$(readlink store/latest 2>/dev/null || true)
    local exit_code=0
    lein run test "$@" || exit_code=$?
    latest=$(readlink store/latest 2>/dev/null || true)
    [[ "$latest" != "$previous" ]] || latest=""
    (( exit_code == 0 )) || failed=1
    append_result "$name" "$exit_code" "$latest"
    python3 "$SCRIPT_DIR/generate_static_report.py" "$REPORT_FILE" ||
        { printf "Failed to generate HTML report for %s\n" "$name" >&2; failed=1; }
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

run_test ydb-serializable "ydb-serializable (row)" \
    "${COMMON[@]}" \
    --model ydb-serializable \
    --with-opindex \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type row

run_test ydb-serializable "ydb-serializable (column)" \
    "${COMMON[@]}" \
    --model ydb-serializable \
    --with-opindex \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type column

run_test snapshot-isolation "snapshot-isolation (row)" \
    "${COMMON[@]}" \
    --model snapshot-isolation \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type row

run_test snapshot-isolation "snapshot-isolation (column)" \
    "${COMMON[@]}" \
    --model snapshot-isolation \
    --batch-single-ops \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --store-type column

run_test read-committed "read-committed (row)" \
    "${COMMON[@]}" \
    --model read-committed \
    --workload-name append-single-row \
    --store-type row

run_test read-committed "read-committed (column)" \
    "${COMMON[@]}" \
    --model read-committed \
    --workload-name append-single-row \
    --store-type column

echo ""
echo "Report: $REPORT_FILE"

exit "$failed"
