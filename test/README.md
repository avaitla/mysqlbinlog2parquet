# Quickstart: build and run against a sample binlog

This directory contains a few small, self-documenting binlog samples in
`samples/` that you can use to smoke-test the tool end-to-end in under a
minute. Each sample is named after the scenario that produced it (the
SQL files in `scenarios/`), so it's obvious from the filename what's
inside.

```
samples/
├── 01-basic-crud.binlog              # INSERT x3, UPDATE x1, DELETE x1 on `items`
├── 02-multi-statement-txn.binlog     # multi-row, multi-statement transaction
└── 03-mixed-types.binlog             # exercises a wider range of MySQL column types
```

## 1. Build the uber-jar

From the repo root:

```bash
make build
# (equivalent to: ./mvnw -DskipTests package)
```

That produces `target/binlog2parquet-0.1.0-all.jar` — the shaded fat-jar
containing all dependencies. JDK 11+ is required (we test against JDK
21, the latest LTS).

## 2. Run against a bundled sample

```bash
make run \
    INPUT=test/samples/01-basic-crud.binlog \
    OUTPUT=/tmp/binlog2parquet-out
```

The output path is treated as a directory root; one Parquet file per
source table lands inside it:

```
/tmp/binlog2parquet-out/
└── binlogtest.items.parquet
```

You should see log lines like:

```
Server    ID: 1
Binary   Log: 01-basic-crud.binlog
Merged GTIDs: ...
Exporting 5 events to: /tmp/binlog2parquet-out
=====================================
         PROCESSING STATISTICS
=====================================
Total Events: 16
Event Types:
  TABLE_MAP: 5
  EXT_WRITE_ROWS: 3
  EXT_UPDATE_ROWS: 1
  EXT_DELETE_ROWS: 1
  ...
Table Operations:
  binlogtest.items: writes=3, updates=1, deletes=1, total=5
```

## 3. Verify the output with DuckDB

```bash
duckdb -c "
  SELECT timestamp_string, event, \"table\", data, old, changed
  FROM read_parquet('/tmp/binlog2parquet-out/*.parquet')
  ORDER BY position;
"
```

Five rows back: three `EXT_WRITE_ROWS` (the inserts), one
`EXT_UPDATE_ROWS` with an `old`/`new` diff in `changed`, and one
`EXT_DELETE_ROWS` carrying the deleted row's pre-image in `old`.

A per-table-and-event-type summary:

```bash
duckdb -c "
  SELECT \"table\", event, COUNT(*) AS n
  FROM read_parquet('/tmp/binlog2parquet-out/*.parquet')
  GROUP BY \"table\", event
  ORDER BY n DESC;
"
```

## 4. Try query fingerprinting (optional)

Spin up any MySQL 8 you can reach (a throwaway sidecar works) and pass
its JDBC URL via `DIGEST_MYSQL`:

```bash
docker run -d --rm --name digest-mysql \
    -e MYSQL_ROOT_PASSWORD=rootpw \
    -p 13306:3306 \
    mysql:8.0

# Wait a few seconds for it to come up, then:
make run \
    INPUT=test/samples/01-basic-crud.binlog \
    OUTPUT=/tmp/binlog2parquet-out \
    DIGEST_MYSQL="jdbc:mysql://127.0.0.1:13306/sys?user=root&password=rootpw&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"

docker rm -f digest-mysql
```

Now `query_fingerprint` and `query_hash` are populated. To find newly
introduced query shapes:

```bash
duckdb -c "
  SELECT query_hash, MIN(timestamp_string) AS first_seen, COUNT(*) AS rows_affected
  FROM read_parquet('/tmp/binlog2parquet-out/*.parquet')
  WHERE query_hash IS NOT NULL
  GROUP BY 1
  ORDER BY first_seen DESC
  LIMIT 20;
"
```

See the top-level [README — Query fingerprinting](../README.md#query-fingerprinting-optional)
for the full rationale.

## 5. Run via Docker (no host JDK / Maven required)

If you don't want to install a JDK or Maven on the host, the
[`Dockerfile`](../Dockerfile) does both the build *and* the run inside
containers. The host needs only `docker`.

```bash
docker build -t binlog2parquet .

docker run --rm \
    -v "$PWD/test/samples:/input:ro" \
    -v "$PWD/out:/output" \
    binlog2parquet \
    /input/01-basic-crud.binlog /output
```

The Parquet file(s) land in `./out/` on the host.

## 6. Regenerate fresh samples

To regenerate `test/samples/*` from a fresh, throwaway MySQL 8 container,
run the harness — it boots a container with all required binlog settings
(`binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`,
`binlog_rows_query_log_events=ON`, `gtid_mode=ON`,
`enforce_gtid_consistency=ON`), replays every `scenarios/*.sql`, rotates
the log between scenarios, and copies the resulting binlogs out:

```bash
make seed-tests   # or: ./test/generate-binlogs.sh
```

Requirements: `docker` and a local `mysql` client.

---

For full documentation (Parquet schema, multi-row caveats,
DDL-is-skipped, S3 deployment, JVM heap tuning), see the top-level
[`README.md`](../README.md).
