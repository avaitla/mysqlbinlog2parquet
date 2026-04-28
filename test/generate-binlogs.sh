#!/usr/bin/env bash
#
# Spin up a throwaway MySQL 8 container, replay every scenario in test/scenarios/
# against it, then copy the resulting binlog files out to test/samples/.
#
# Each scenario lands in its own dedicated binlog file (FLUSH BINARY LOGS is
# issued between scenarios). MySQL's initial bootstrap output is intentionally
# discarded — we wait for the server to be quiet, then rotate the log a few
# times before running the first scenario, so the exported binlogs contain
# only scenario DML.
#
# The exported files are renamed to match the scenario filename so they are
# self-documenting:
#
#   test/samples/01-basic-crud.binlog
#   test/samples/02-multi-statement-txn.binlog
#   test/samples/03-mixed-types.binlog
#
# Requirements:
#   - docker
#   - mysql client (`brew install mysql-client` / `apt-get install mysql-client`)
#
set -euo pipefail

CONTAINER=binlog2parquet-mysql
HOST_PORT=${HOST_PORT:-13306}
ROOT_PASSWORD=${ROOT_PASSWORD:-rootpw}
DATABASE=${DATABASE:-binlogtest}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIOS_DIR="$ROOT_DIR/test/scenarios"
SAMPLES_DIR="$ROOT_DIR/test/samples"

mkdir -p "$SAMPLES_DIR"

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[harness] Starting MySQL container..."
docker run -d --rm \
    --name "$CONTAINER" \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PASSWORD" \
    -e MYSQL_DATABASE="$DATABASE" \
    -p "${HOST_PORT}:3306" \
    mysql:8.0 \
    --server-id=1 \
    --log-bin=mysql-bin \
    --binlog-format=ROW \
    --binlog-row-image=FULL \
    --binlog-row-metadata=FULL \
    --binlog-rows-query-log-events=ON \
    --gtid-mode=ON \
    --enforce-gtid-consistency=ON \
    >/dev/null

echo "[harness] Waiting for MySQL to accept connections on :${HOST_PORT}..."
for _ in $(seq 1 60); do
    if mysql -h 127.0.0.1 -P "$HOST_PORT" -uroot -p"$ROOT_PASSWORD" -e 'SELECT 1' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

mysql_exec() {
    mysql -h 127.0.0.1 -P "$HOST_PORT" -uroot -p"$ROOT_PASSWORD" "$@"
}

# Drain the bootstrap noise: MySQL's initial schema setup keeps writing for a
# moment after the server accepts connections. Sleep briefly to let it quiesce,
# then rotate the binlog a couple of times so the bootstrap content is left
# behind in early, unexported files.
sleep 3
mysql_exec -e "FLUSH BINARY LOGS;"
sleep 1
mysql_exec -e "FLUSH BINARY LOGS;"

# Remember which binlog is active right now — that's the one the FIRST scenario
# will land in. Anything strictly before it is bootstrap and gets discarded.
first_scenario_log=$(mysql_exec -BNe "SHOW MASTER STATUS;" | awk '{print $1}')
echo "[harness] First scenario will land in: $first_scenario_log"

# Map each scenario filename to a label, and remember which binlog it produced.
declare -a labels
declare -a logs

shopt -s nullglob
for scenario in "$SCENARIOS_DIR"/*.sql; do
    label=$(basename "$scenario" .sql)
    active=$(mysql_exec -BNe "SHOW MASTER STATUS;" | awk '{print $1}')
    echo "[harness] Running scenario $label -> $active"
    mysql_exec "$DATABASE" < "$scenario"
    labels+=("$label")
    logs+=("$active")
    # Roll the binlog so the next scenario starts in a fresh file.
    mysql_exec -e "FLUSH BINARY LOGS;"
done

echo "[harness] Copying binlog files out of the container..."
for i in "${!labels[@]}"; do
    label="${labels[$i]}"
    src="${logs[$i]}"
    dest="$SAMPLES_DIR/${label}.binlog"
    echo "  -> $dest"
    docker cp "$CONTAINER:/var/lib/mysql/${src}" "$dest"
done

echo "[harness] Done. Generated samples:"
ls -lh "$SAMPLES_DIR"
