#!/usr/bin/env bash
#
# Spin up a throwaway MySQL 8 container, replay every scenario in test/scenarios/
# against it, then copy the resulting binlog files out to test/samples/.
#
# Each scenario is given its own binlog file by issuing FLUSH BINARY LOGS
# before and after it.
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

# Ensure the very first scenario starts in its own binlog file.
mysql_exec -e "FLUSH BINARY LOGS;"

shopt -s nullglob
for scenario in "$SCENARIOS_DIR"/*.sql; do
    name=$(basename "$scenario" .sql)
    echo "[harness] Running scenario: $name"
    mysql_exec "$DATABASE" < "$scenario"
    # Roll the binlog so each scenario produces a separate file.
    mysql_exec -e "FLUSH BINARY LOGS;"
done

echo "[harness] Listing binlogs:"
mysql_exec -e "SHOW BINARY LOGS;"

echo "[harness] Copying binlog files out of the container..."
# Skip the currently-active log; we only export rotated (completed) ones.
docker exec "$CONTAINER" bash -c \
    "ls /var/lib/mysql/mysql-bin.[0-9]* | head -n -1" \
    | while read -r path; do
        fname=$(basename "$path")
        echo "  -> $SAMPLES_DIR/$fname"
        docker cp "$CONTAINER:$path" "$SAMPLES_DIR/$fname"
    done

echo "[harness] Done."
