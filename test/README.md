# Quickstart: build and run against a sample binlog

This directory contains a checked-in sample binlog file
(`samples/mysql-bin-changelog.547927`) you can use to smoke-test the tool
end-to-end in under a minute.

## 1. Build the uber-jar

From the repo root:

```bash
./mvnw -DskipTests package
```

That produces `target/binlog2parquet-0.1.0-all.jar` (the shaded
fat-jar containing all dependencies). JDK 11+ is required.

## 2. Run against the bundled sample

```bash
mkdir -p /tmp/binlog2parquet-out

java -jar target/binlog2parquet-0.1.0-all.jar \
    test/samples/mysql-bin-changelog.547927 \
    /tmp/binlog2parquet-out
```

Because the second argument points at an existing directory, the tool
auto-names the output as
`mysql-bin-changelog.<serverId>.<paddedSeq>` inside it, e.g.
`/tmp/binlog2parquet-out/mysql-bin-changelog.1.0000547927`.

You should see log lines like:

```
[MemoryMonitor] PROCESS_START - Heap: 248MB used / ...
Server    ID: ...
Binary   Log: mysql-bin-changelog.547927
Merged GTIDs: ...
Exporting <N> events to: /tmp/binlog2parquet-out/...
=====================================
         PROCESSING STATISTICS
=====================================
Total Events: ...
Event Types:
  EXT_WRITE_ROWS: ...
  EXT_UPDATE_ROWS: ...
  ...
```

## 3. Verify the output

A reference Parquet file produced from the same input is committed at
`samples/mysql-bin-changelog.547927.parquet` for comparison. Quick
sanity check with DuckDB:

```bash
duckdb -c "SELECT event, COUNT(*) FROM read_parquet('/tmp/binlog2parquet-out/*') GROUP BY 1 ORDER BY 2 DESC;"
```

Or with `pqrs` / `parquet-tools`:

```bash
pqrs schema /tmp/binlog2parquet-out/mysql-bin-changelog.1.0000547927
pqrs head   /tmp/binlog2parquet-out/mysql-bin-changelog.1.0000547927 -n 5
```

## 4. Try query fingerprinting (optional)

Spin up any MySQL 8 you can reach — including the throwaway one from
`generate-binlogs.sh` — and pass its JDBC URL:

```bash
docker run -d --rm --name digest-mysql \
    -e MYSQL_ROOT_PASSWORD=rootpw \
    -p 13306:3306 \
    mysql:8.0

# Wait a few seconds for it to come up, then:
java -jar target/binlog2parquet-0.1.0-all.jar \
    --digest-mysql="jdbc:mysql://127.0.0.1:13306/sys?user=root&password=rootpw&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
    test/samples/mysql-bin-changelog.547927 \
    /tmp/binlog2parquet-out
```

Now `query_fingerprint` and `query_hash` are populated. To find newly
introduced query shapes:

```bash
duckdb -c "
SELECT query_hash, MIN(timestamp_string) AS first_seen, COUNT(*) AS rows_affected
FROM read_parquet('/tmp/binlog2parquet-out/*')
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
# Build inside Docker (skips ./mvnw entirely; no host JDK needed).
docker build -t binlog2parquet .

# Run, mounting the sample dir read-only and an output dir read/write.
docker run --rm \
    -v "$PWD/test/samples:/input:ro" \
    -v "$PWD/out:/output" \
    binlog2parquet \
    /input/mysql-bin-changelog.547927 /output
```

The Parquet file lands in `./out/` on the host.

To also run the digest sidecar from step 4 without touching the host
beyond Docker:

```bash
docker network create b2p-net

docker run -d --rm --name digest-mysql --network b2p-net \
    -e MYSQL_ROOT_PASSWORD=rootpw mysql:8.0

docker run --rm --network b2p-net \
    -v "$PWD/test/samples:/input:ro" \
    -v "$PWD/out:/output" \
    binlog2parquet \
    --digest-mysql="jdbc:mysql://digest-mysql:3306/sys?user=root&password=rootpw&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
    /input/mysql-bin-changelog.547927 /output

docker rm -f digest-mysql
docker network rm b2p-net
```

## 6. (Optional) Generate fresh samples

To regenerate `test/samples/*` from a fresh, throwaway MySQL 8 container,
run the harness — it boots a container with all required binlog settings
(`binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`,
`binlog_rows_query_log_events=ON`, `gtid_mode=ON`,
`enforce_gtid_consistency=ON`), replays every `scenarios/*.sql`, rotates
the log between scenarios, and copies the resulting binlogs out:

```bash
./test/generate-binlogs.sh
```

Requirements: `docker` and a local `mysql` client.

---

For full documentation (Parquet schema, multi-row caveats,
DDL-is-skipped, S3 deployment, JVM heap tuning), see the top-level
[`README.md`](../README.md).
