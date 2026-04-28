# binlog2parquet

A small Java CLI that reads a MySQL binary log file and writes a Parquet
representation of every event in it (row events, GTIDs, query events, etc.)
suitable for downstream analytics.

It is built on top of
[`mysql-binlog-connector-java`](https://github.com/osheroff/mysql-binlog-connector-java)
(consumed as a regular Maven dependency) plus Apache Parquet.

---

## Quickstart — convert a sample binlog and query it with DuckDB

The repo ships with a few pre-generated MySQL 8 binlog samples in
`test/samples/`. The simplest one — `01-basic-crud.binlog` — was
produced by replaying [`test/scenarios/01-basic-crud.sql`](test/scenarios/01-basic-crud.sql)
against a throwaway MySQL 8 container. It writes to **two** tables on
purpose, so the quickstart also demonstrates that the converter
emits **one Parquet file per source table**:

```sql
-- test/scenarios/01-basic-crud.sql
CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(64) NOT NULL,
    qty INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob',   'bob@example.com');

INSERT INTO items (sku, qty) VALUES
    ('sku-0001', 10),
    ('sku-0002', 25),
    ('sku-0003', 5);

UPDATE items SET qty = 11 WHERE sku = 'sku-0001';
UPDATE customers SET email = 'alice+new@example.com' WHERE name = 'Alice';
DELETE FROM items WHERE sku = 'sku-0002';
```

That's five INSERTs, two UPDATEs, one DELETE — eight row events split
across two tables. End-to-end, on a host with JDK 11+ and
[`duckdb`](https://duckdb.org/) installed (`brew install duckdb` /
`apt-get install duckdb` / single-binary download):

```bash
# 1. Build the shaded uber-jar.
make build
# (equivalent to: ./mvnw -DskipTests package)

# 2. Convert a sample binlog. The OUTPUT path is the *root* of an output
#    directory tree — the tool writes one Parquet file per source table
#    inside it (see "Output layout" below).
make run \
    INPUT=test/samples/01-basic-crud.binlog \
    OUTPUT=/tmp/binlog2parquet/01-basic-crud

# Result: one Parquet file per source table — note the two files,
# even though the input is a single binlog.
ls /tmp/binlog2parquet/01-basic-crud/
# -> binlogtest.customers.parquet
# -> binlogtest.items.parquet

# 3. Query the Parquet directly with DuckDB — no schema, no loader, no
#    extra service to stand up. DuckDB reads Parquet natively, so a
#    single CLI command is enough to inspect what the binlog captured.
#    The glob pulls in both per-table files at once; the per-row "table"
#    column tells you which one each row came from.
duckdb -c "
  SELECT timestamp_string, event, \"table\", data, old, changed
  FROM read_parquet('/tmp/binlog2parquet/01-basic-crud/*.parquet')
  ORDER BY \"table\", position;
"
```

Expected DuckDB output (timestamps will differ; row contents will not):

```
┌──────────────────────────┬─────────────────┬──────────────────────┬─────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────┐
│     timestamp_string     │      event      │        table         │                                      data                                       │                                       old                                       │                               changed                               │
├──────────────────────────┼─────────────────┼──────────────────────┼─────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ 2026-04-28 17:14:01-05   │ EXT_WRITE_ROWS  │ binlogtest.customers │ {"id":1,"name":"Alice","email":"alice@example.com"}                             │ NULL                                                                            │ NULL                                                                │
│ 2026-04-28 17:14:01-05   │ EXT_WRITE_ROWS  │ binlogtest.customers │ {"id":2,"name":"Bob","email":"bob@example.com"}                                 │ NULL                                                                            │ NULL                                                                │
│ 2026-04-28 17:14:01-05   │ EXT_UPDATE_ROWS │ binlogtest.customers │ {"id":1,"name":"Alice","email":"alice+new@example.com"}                         │ {"id":1,"name":"Alice","email":"alice@example.com"}                             │ {"email":{"old":"alice@example.com","new":"alice+new@example.com"}} │
│ 2026-04-28 17:14:01-05   │ EXT_WRITE_ROWS  │ binlogtest.items     │ {"id":1,"sku":"sku-0001","qty":10,"created_at":"2026-04-28T22:14:01.000+00:00"} │ NULL                                                                            │ NULL                                                                │
│ 2026-04-28 17:14:01-05   │ EXT_WRITE_ROWS  │ binlogtest.items     │ {"id":2,"sku":"sku-0002","qty":25,"created_at":"2026-04-28T22:14:01.000+00:00"} │ NULL                                                                            │ NULL                                                                │
│ 2026-04-28 17:14:01-05   │ EXT_WRITE_ROWS  │ binlogtest.items     │ {"id":3,"sku":"sku-0003","qty":5,"created_at":"2026-04-28T22:14:01.000+00:00"}  │ NULL                                                                            │ NULL                                                                │
│ 2026-04-28 17:14:01-05   │ EXT_UPDATE_ROWS │ binlogtest.items     │ {"id":1,"sku":"sku-0001","qty":11,"created_at":"2026-04-28T22:14:01.000+00:00"} │ {"id":1,"sku":"sku-0001","qty":10,"created_at":"2026-04-28T22:14:01.000+00:00"} │ {"qty":{"old":10,"new":11}}                                         │
│ 2026-04-28 17:14:01-05   │ EXT_DELETE_ROWS │ binlogtest.items     │ {"id":2,"sku":"sku-0002","qty":25,"created_at":"2026-04-28T22:14:01.000+00:00"} │ {"id":2,"sku":"sku-0002","qty":25,"created_at":"2026-04-28T22:14:01.000+00:00"} │ NULL                                                                │
└──────────────────────────┴─────────────────┴──────────────────────┴─────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

Notice that:

- One input binlog produced **two** Parquet files —
  `binlogtest.customers.parquet` and `binlogtest.items.parquet` —
  because the converter shards the output by source table. The
  `"table"` column on each row tells you which file it came from.
- The five `INSERT`s appear as `EXT_WRITE_ROWS` with `data` = the new
  row image and `old` / `changed` = NULL.
- The two `UPDATE`s appear as `EXT_UPDATE_ROWS` with `data` =
  post-image, `old` = pre-image, and `changed` = a JSON diff
  (`{"qty":{"old":10,"new":11}}` /
  `{"email":{"old":"alice@example.com","new":"alice+new@example.com"}}`)
  — exactly what you'd want to power a time-travel query.
- The `DELETE` appears as `EXT_DELETE_ROWS` with `old` = the row that
  disappeared.

That's the whole pipeline: **`binlog file → make run → Parquet → DuckDB`**.

The same Parquet files work equally well in Spark, Trino, ClickHouse,
Snowflake (`COPY INTO`), Athena, or Polars — see
[Output Parquet schema](#output-parquet-schema) for the column list and
[Per-event-type semantics](#per-event-type-semantics) for what each
event populates. Pick whichever engine your team already runs.

### Output layout

Whatever path you pass as `OUTPUT`, the tool treats it as a **directory
root** (stripping a trailing `.parquet` if present) and writes one
Parquet file per source table inside it:

```
<OUTPUT>/
├── <db>.<tableA>.parquet
├── <db>.<tableB>.parquet
└── …
```

The per-table split is deliberate — it gives Parquet's column-oriented
encoding a much better shot at compressing the output. Within a single
table file, `data` / `old` / `changed` rows share the same column set,
JSON key order, and value distributions, so dictionary encoding,
run-length encoding, and Snappy all compress aggressively. Mixing
events from many tables into one file destroys that locality: the
column values look effectively random, dictionaries blow up, and the
file ends up substantially larger.

If you'd rather emit a single combined file anyway, the project is
designed to be modified: `ParquetExporter.export` is one short method,
and removing the `groupEventsByTable(...)` call collapses everything
back to one file.

If you don't have a JDK on the host, swap step 1 for the [Docker
build](#docker-zero-host-install) — same output, no host install.

---

## Motivation

### 1. Get the full pre- and post-image of every row, in a queryable file

The MySQL binlog (in `binlog_format=ROW`, with `binlog_row_image=FULL`)
already contains everything you need to reconstruct *time-travel* style
queries on your data: the exact row state **before** and **after** every
INSERT / UPDATE / DELETE, identified by GTID and tied to the original
SQL statement. The catch is that the binlog is a binary, sequential
format — you can `mysqlbinlog | grep` it, but you can't `SELECT … WHERE
… GROUP BY` it without parsing every byte first.

Materializing each row event as a single Parquet row, with `data` (post-
image), `old` (pre-image), and `changed` (diff) columns alongside GTID
and table metadata, turns the binlog into something DuckDB / Spark /
Trino / ClickHouse / Snowflake can query directly. That unlocks the
analyses the binlog was *already* capable of supporting but in practice
nobody runs against the raw format:

- "What did this row look like an hour before the bad deploy?"
- "Which tables had the highest write churn last week?"
- "Show me every UPDATE where `status` flipped from `active` to
  `cancelled`, regardless of which service issued it."
- "When was column X first set to value Y, by which transaction?"

### 2. Spot new query shapes and find your hottest tables

The same Parquet output makes it cheap to answer two questions that are
otherwise hard to get good signal on from a live database:

- **"Are there new query shapes hitting prod that I didn't expect?"**
  When you pass `--digest-mysql=<jdbc-url>`, every distinct SQL
  statement in the binlog is run through MySQL's
  `STATEMENT_DIGEST_TEXT()` / `STATEMENT_DIGEST()`, which collapses
  literals down to a normalized shape and a stable hash (see
  [Query fingerprinting](#query-fingerprinting-optional)). Group the
  resulting Parquet by `query_hash` with `min(timestamp)` and any hash
  whose first appearance is **recent** is, by definition, a brand-new
  query shape — a feature rollout, a misbehaving ORM, a forgotten ad-hoc
  reporting query, an unauthorized integration. This is the highest-
  signal way we know to detect "something changed in the workload"
  without having to instrument the application.
- **"Which tables are taking the brunt of writes?"** Every Parquet row
  carries a fully-qualified `table` plus the event type (`EXT_WRITE_ROWS`
  / `EXT_UPDATE_ROWS` / `EXT_DELETE_ROWS`), so a one-line
  `GROUP BY table, event` tells you which tables receive the most
  inserts, updates, and deletes over any window you care about. We use
  this to:
    - decide where to **add audit triggers** (high-write tables that
      currently have no audit trail are the obvious candidates),
    - find **unexpected usage** patterns (a "settings" table that's
      receiving 100K updates/hour is almost certainly being misused as
      a queue),
    - spot **delete storms** (sudden DELETE volume on a table that is
      conceptually append-only),
    - prioritize **schema cleanup** (tables that show no writes at all
      across a long binlog window are candidates for deprecation).

Both analyses are basically free once the binlog is sitting in Parquet
on S3 — DuckDB / ClickHouse / Snowflake / Athena will run them in
seconds — and neither requires touching the live database.

### 3. Decouple **archiving** from **processing**

This tool is intentionally just step 2 of a two-step pipeline:

```
  MySQL ──[archive]──► S3 (raw binlog files) ──[process]──► S3 (parquet)
```

The archiver is trivial — `mysqlbinlog --read-from-remote-server --raw`
(or any equivalent) writes whole binlog files to a directory; point that
directory at an S3 mount and the file lands in the bucket on close. No
schema, no parser, almost no failure modes.

The processor (this tool) is much more complex: it has to interpret the
binlog format, reconstruct row images from `TABLE_MAP` events, group
rows under GTIDs, handle every column type MySQL emits, and produce a
schema'd Parquet file. Bugs here are inevitable — a new MySQL version,
a previously-unseen column type, an OOM on a wide-table workload, a
fix-forward that changes the output schema.

**Combining the two steps is the failure mode we keep seeing in CDC
systems.** When the archiver and processor are the same daemon, a
processor bug can:

- block the archiver (back-pressure stalls the binlog read)
- crash the daemon (so binlogs that *would* have been safely archived
  never get pulled, and source MySQL eventually purges them)
- corrupt downstream state in a way that requires re-reading from the
  source — except the source has rotated past the affected window

**Splitting them removes that whole class of incident.** The archiver
job is simple, easy to monitor, and easy to make idempotent and durable:
once a closed binlog file is in S3, it's safe forever. The processor
can fail, get rewritten, regress, get bumped to a new schema, fall hours
behind, or be replaced wholesale — the archived binlogs in S3 are
unaffected, so you can always reprocess from the beginning, or from any
point you choose.

This tool is the processor half. The archive half is one shell command
you run on a sidecar (see
[Pulling binlogs from a live MySQL into the mount](#3-pulling-binlogs-from-a-live-mysql-into-the-mount)).

### 4. No S3 SDK, no extra dependencies — just files

Both halves treat S3 as a regular POSIX filesystem via
[**S3 Files**](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-mounting.html)
(AWS-native NFS mount), [Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3),
[`s3fs-fuse`](https://github.com/s3fs-fuse/s3fs-fuse), or
[`rclone mount`](https://rclone.org/commands/rclone_mount/). The archiver
writes binlogs into a mounted directory; this tool reads from one mounted
directory and writes Parquet to another. There is **no AWS SDK in the jar,
no S3 client config, no credentials wired into the application** — and
no equivalent layer for whatever object store you swap in (GCS via
`gcsfuse`, Azure via `blobfuse`, on-prem MinIO over `s3fs`, a local SSD,
an NFS share). The application code only ever opens files.

That has knock-on benefits beyond "it's simpler":

- **Smaller dependency surface** to audit and keep up to date — no
  `aws-java-sdk-*` (or `software.amazon.awssdk:*`) transitive tree, no
  recurring CVEs in storage-client libraries, no tightly-coupled SDK
  upgrades when the storage backend changes.
- **Easy local development.** A developer on a laptop runs the tool
  against `./binlogs/` exactly the same way prod runs it against
  `/mnt/s3-binlogs/`. No fake-S3 emulator, no environment-specific code
  paths, no `if (s3) { ... } else { ... }` branches.
- **Storage-backend portability** falls out for free. Want to migrate
  from `s3fs-fuse` to Mountpoint for higher throughput? From
  Mountpoint to S3 Files for AWS-native semantics? From S3 to GCS?
  Change the mount, ship nothing.
- **Operational levers stay where ops already lives**: IAM policies on
  the bucket, instance profiles on the VM, mount options in `/etc/fstab`,
  `aws s3 cp` for inspection. None of that has to be re-implemented or
  re-configured inside the converter.

---

## Status

The tool is intentionally narrow: **one input binlog file → one Parquet
output**. There is no daemon, no web service, and no embedded S3 client.
S3-backed inputs and outputs are handled by mounting the bucket as a regular
filesystem (see [Deployment](#deployment)).

The conversion pipeline has been exercised in production on real-world
MySQL workloads — including pathological binlogs whose parsing required
**>150 GB of JVM heap**. Plan to size `-Xmx` to your workload (see
[Memory monitoring](#memory-monitoring)).

---

## Tradeoffs and known downsides

Before adopting this tool, be aware of the following — most are direct
consequences of the "JVM + Apache Parquet" implementation choice:

- **The shaded jar is fat (~65–70 MB).** Apache Parquet's Java writer
  pulls in `parquet-hadoop`, which transitively drags in much of the
  Hadoop client stack (`hadoop-common`, `hadoop-auth`, Kerberos / JAAS
  bits, `commons-*`, `guava`, `protobuf`, `woodstox`, `re2j`, …). None
  of that is logically required to write a Parquet file, but the writer
  is layered on top of Hadoop's `FileSystem` / `Configuration`
  abstractions, so it ships with them. The runtime artifact is
  correspondingly large compared to a "pure" Parquet writer in Rust /
  C++ / Go (typically a few MB or a single static binary).
  - The same coupling means startup cost is JVM-class — expect a
    second or two of JVM + Hadoop initialization before the first byte
    of the binlog is read. Fine for batch processing whole binlog files
    (the use case this tool is built for); a poor fit for short-lived
    invocations on tiny inputs.
  - If shipping size is critical (e.g. embedding into another tool, or
    distributing to many edge hosts), consider one of the lighter-weight
    Parquet implementations — [Apache Arrow Parquet (C++)](https://arrow.apache.org/docs/cpp/parquet.html),
    [`parquet` (Rust)](https://crates.io/crates/parquet), or
    [`pyarrow`](https://arrow.apache.org/docs/python/parquet.html) —
    and call this tool only for binlog parsing, or port the binlog →
    row-image logic to that ecosystem.

- **Heap requirements scale with binlog metadata, not just data.**
  `binlog_row_metadata=FULL` (which is required for column names) embeds
  a full copy of the table's column metadata in *every* row event. On
  wide tables and long binlogs that means JVM heap can be many multiples
  of the on-disk binlog size — we have seen single files require
  >150 GB of heap to convert. There is no streaming/spill-to-disk
  fallback in the current code path; if the heap is undersized, the
  process OOMs and the run is lost. See
  [Memory monitoring](#memory-monitoring) for sizing guidance.

- **One file in, one directory out — no incremental / streaming mode.**
  The tool processes a single closed binlog file end-to-end. There is no
  tail-following mode, no resume-from-offset, no checkpointing. If a
  conversion fails partway through, you re-run the whole file. This is
  intentional (see [Decouple archiving from processing](#3-decouple-archiving-from-processing))
  but it does mean that very large individual binlog files have a
  proportionally large blast radius on a failed run.

- **Values are JSON-encoded inside `data` / `old`, not typed Parquet
  columns.** The output schema is flat and table-agnostic; per-column
  types are not preserved as Parquet types. Downstream queries have to
  use engine-native JSON functions (DuckDB `json_extract`, Spark
  `from_json`, etc.) and treat numeric / binary values defensively. See
  [Caveats](#caveats) under the schema section for the full list.

- **DDL never appears in the output.** `ALTER` / `CREATE` / `DROP`
  arrive as `QUERY` events and are dropped on the floor. If you need a
  record of schema changes, capture them out-of-band.

---

## Possible extensions

The tool is intentionally narrow — one binlog file in, one directory
of Parquet out — but a few directions are obvious next steps if you
want to fork it. None of these are implemented today; they are
documented here as extension points, with pointers to the code you'd
touch.

- **Run as a long-lived web service rather than a one-shot CLI.** The
  current entry point ([`Main.main`](src/main/java/io/github/binlog2parquet/Main.java))
  parses argv and exits. Wrapping the same `BinlogReader` →
  `ParquetExporter` pipeline in a small HTTP server (e.g.
  Javalin / Spring Boot / `com.sun.net.httpserver.HttpServer`) would
  let an upstream archiver `POST` a binlog path (or upload bytes) and
  get back a manifest of the per-table Parquet files. The conversion
  logic itself does not need to change — only the framing around it.
  The same long-lived process could keep the JVM warm across
  conversions, which materially reduces the second-or-two of
  Hadoop / JVM start-up cost noted in [Tradeoffs](#tradeoffs-and-known-downsides).

- **Column- or row-level encryption on the output.** Parquet has
  native modular encryption (`PARQUET-1396`) which the underlying
  `parquet-hadoop` writer already supports — the hook would be in
  [`ParquetExporter.export`](src/main/java/io/github/binlog2parquet/ParquetExporter.java),
  where the `ParquetWriter` is built. Pass an encryption properties
  builder configured with a KMS-backed key retriever and the writer
  will emit encrypted column chunks. A common shape is "encrypt
  `data` / `old` / `changed` (the user-data columns) but leave `gtid`
  / `position` / `table` clear" so downstream queries can still index
  and filter without unwrapping the secret material.

- **Up-front allow-list of tables and columns.** The tool currently
  emits every row event for every table in the binlog, with all
  columns inlined into the `data` / `old` JSON. Two filters would let
  consumers cut volume (and exposure) significantly:
    1. A **table allow-list / deny-list** applied where events are
       grouped (`groupEventsByTable(...)` in `ParquetExporter`) — drop
       events for unlisted tables before they ever reach a writer.
    2. A **per-table column projection / redaction list** applied
       where the row image is materialized into JSON
       ([`EventWrapper`](src/main/java/io/github/binlog2parquet/EventWrapper.java),
       which Jackson-serializes the row into the `data` / `old` /
       `changed` strings). Unlisted columns get omitted (or replaced
       with a hash / sentinel) so PII never lands in Parquet in the
       first place.
  Both are local edits — the schema and the rest of the pipeline are
  unchanged.

- **Delete the source binlog after a successful conversion.** Today
  `Main` reads the binlog and writes Parquet; it never touches the
  input afterward. If your archiver is durable (e.g. the binlog is
  S3-resident under a versioned bucket and the converter has already
  copied it to a "processed" prefix), an opt-in
  `--delete-input-on-success` flag at the end of `Main.main` is a
  one-liner. Worth gating on (a) the Parquet writer having closed
  cleanly without exceptions, and (b) the output files being non-zero
  size — both are observable from `ParquetExporter.export`'s return
  path. For safety, keep this **off by default**; a stale archive is
  recoverable, a deleted-and-not-converted binlog is not.

These are deliberately scoped as extensions, not roadmap items — the
core tool is meant to stay small enough to read end-to-end. If you
implement any of them, the seams above are where to cut.

---

## Requirements on the source MySQL server

The Parquet output captures the full pre/post image of every row, with
column names. That requires the source MySQL server to be configured so the
binlog contains enough information to reconstruct it. **The following
settings are mandatory** — if any of them is missing, the input binlog will
either fail to parse or will parse with empty/placeholder column data:

| Setting                            | Required value | Why                                                  |
| ---------------------------------- | -------------- | ---------------------------------------------------- |
| `binlog_format`                    | `ROW`          | Statement-based binlogs cannot be replayed faithfully |
| `binlog_row_image`                 | `FULL`         | Both pre- and post-images of every changed row       |
| `binlog_row_metadata`              | `FULL`         | Embeds column names, signedness, charsets            |
| `binlog_rows_query_log_events`     | `ON`           | Captures the original SQL alongside each row event    |
| `gtid_mode`                        | `ON`           | GTID merging in the Parquet output requires GTIDs    |
| `enforce_gtid_consistency`         | `ON`           | Required for GTID-mode                               |

> **⚠️ Memory note.** `binlog_row_metadata=FULL` is the right setting, but it
> can dramatically inflate per-event metadata, especially on wide tables.
> A binlog whose on-disk size is a few hundred MB can easily require several
> GB of JVM heap to parse, because every row event carries a full copy of
> the table's column metadata. **Plan to size `-Xmx` generously (4–8 GB is a
> reasonable starting point for production workloads).** The tool ships
> with an integrated [`MemoryMonitor`](src/main/java/io/github/binlog2parquet/MemoryMonitor.java)
> that emits periodic heap-usage reports to stderr so you can tune the heap;
> see [Memory monitoring](#memory-monitoring).

---

## Build

**JDK 11+** is required (we test against the latest LTS — JDK 21 — and
recommend it for new installs; 11 and 17 also work):

```bash
./mvnw -DskipTests package
# -> target/binlog2parquet-0.1.0-all.jar   (shaded uber-jar)
```

> Don't want to install a JDK or Maven on the host? Use the
> [bundled Dockerfile](#docker-zero-host-install) — both the build and
> the runtime live entirely inside Docker.

### "Unable to locate a Java Runtime"

If `./mvnw` exits with:

```
The operation couldn't be completed. Unable to locate a Java Runtime.
Please visit http://www.java.com for information on installing Java.
```

…there is no JDK on `PATH` / `JAVA_HOME`. You have two options.

> **macOS — already `brew install`ed `openjdk@21` (or `@17`/`@11`) and
> still hitting this?** Homebrew deliberately does not link its
> `openjdk` formula into the system Java locations, so `/usr/libexec/java_home`
> and `./mvnw` won't find it until you point at it explicitly:
> ```bash
> export JAVA_HOME=/opt/homebrew/opt/openjdk@21   # or @17 / @11
> export PATH="$JAVA_HOME/bin:$PATH"
> ```
> Add those to `~/.zshrc` to make it stick. (Apple Silicon paths shown;
> Intel Macs use `/usr/local/opt/...`.) If you'd rather have
> `/usr/libexec/java_home` pick it up system-wide, symlink it in:
> `sudo ln -sfn /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk`.

**Option 1 — install a JDK 11+ locally.** JDK 21 (latest LTS) is the
recommended default; 11 and 17 also work.

```bash
# macOS (Homebrew, no sudo, recommended):
brew install openjdk@21
export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
# or, if you prefer the cask (needs sudo for the system-wide install):
#   brew install --cask temurin@21
#   export JAVA_HOME="$(/usr/libexec/java_home -v 21)"

# Ubuntu / Debian:
sudo apt-get update && sudo apt-get install -y openjdk-21-jdk
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Amazon Linux 2023 / RHEL / Fedora:
sudo yum install -y java-21-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto

# Verify:
java -version
./mvnw -DskipTests package
```

Persist `PATH` / `JAVA_HOME` in your shell rc (`~/.zshrc`, `~/.bashrc`)
so future shells pick it up.

**Option 2 — skip the host JDK entirely and build in Docker.** See
[Docker (zero host install)](#docker-zero-host-install) below — `docker
build -t binlog2parquet .` runs the entire Maven build inside a
container, with no `java` or `mvn` on the host.

---

## Usage

```bash
java -jar target/binlog2parquet-0.1.0-all.jar \
    [--digest-mysql=<jdbc-url>] \
    <input-binlog-file> <output-path>
```

- `<input-binlog-file>` — path to a single MySQL binary log file (e.g.
  `mysql-bin.000042`).
- `<output-path>` — if it points to an existing directory, an output
  filename of the form `<basename>.<serverId>.<paddedSeq>` is generated
  inside it (see [Output filename layout](#output-filename-layout) for why
  the sequence is zero-padded). Otherwise the path is used verbatim and
  any missing parent directories are created.
- `--digest-mysql=<jdbc-url>` — *optional*. Enables query fingerprinting;
  see [Query fingerprinting](#query-fingerprinting-optional).

Examples:

```bash
# Explicit output path
java -jar target/binlog2parquet-0.1.0-all.jar \
    /var/lib/mysql/mysql-bin.000042 /tmp/out.parquet

# Auto-named output in an existing directory
mkdir -p /tmp/parquet-out
java -jar target/binlog2parquet-0.1.0-all.jar \
    /var/lib/mysql/mysql-bin.000042 /tmp/parquet-out

# Tune heap for large/wide-row binlogs
java -Xmx8g -jar target/binlog2parquet-0.1.0-all.jar /path/to/binlog /path/to/output

# With query fingerprinting (any reachable MySQL 5.7+ will do)
java -jar target/binlog2parquet-0.1.0-all.jar \
    --digest-mysql="jdbc:mysql://127.0.0.1:3306/sys?user=root&password=secret&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
    /var/lib/mysql/mysql-bin.000042 /tmp/parquet-out
```

### Output filename layout

In directory mode the generated filename is
`<binlog-basename>.<serverId>.<paddedSeq>`, where `<paddedSeq>` is the
trailing numeric segment of the source binlog filename, **left-padded
with zeros to 10 digits** (e.g. `mysql-bin.42` → `…0000000042`).

**Why `<serverId>` is in the name.** Production MySQL deployments usually
have more than one source of binlogs landing in the same place — multiple
databases on different servers, or a primary/replica pair where failovers
mean a given logical "stream" is produced by whichever node is current
master. Embedding the source `server_id` lets you point all of them at
one shared output directory (or one S3 prefix) without filename
collisions: every binlog is uniquely identified by
`(server_id, sequence)`, which is exactly what shows up in the filename.
This is what makes the tool usable in HA topologies — converted Parquet
files from old-primary and new-primary coexist cleanly, and downstream
consumers can group by `server_id` if they need per-source streams.

**Why the sequence is zero-padded.** Third-party ETL systems that ingest
from S3 incrementally — ClickPipes, Snowpipe, Fivetran, Airbyte, etc. —
typically key off filename order (S3 `ListObjects` returns keys in
lexicographic order). With unpadded sequence numbers, `mysql-bin.10`
sorts *before* `mysql-bin.2`, and the ingester silently skips files.
Zero-padding makes lexicographic order = chronological order, so any
system that just "reads keys in sorted order" stays correct as the
binlog sequence crosses each power-of-ten boundary.

If you pass an explicit output filename, you're responsible for naming;
the tool writes verbatim to wherever you point it.

---

## Query fingerprinting (optional)

When you pass `--digest-mysql=<jdbc-url>`, every distinct SQL statement
captured in the binlog (i.e. every `ROWS_QUERY` event) is run through
MySQL's built-in
[`STATEMENT_DIGEST_TEXT()`](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_statement-digest-text)
and `STATEMENT_DIGEST()` functions. The results are written into the
output Parquet as:

- `query_fingerprint` — the **normalized** statement, with literals
  stripped (e.g. `INSERT INTO items (sku, qty) VALUES (?, ?)` instead of
  `INSERT INTO items (sku, qty) VALUES ('sku-0001', 10)`).
- `query_hash` — a stable SHA-256–style fingerprint of that normalized
  text.

### Why bother

The original SQL text in `query` includes literal values, so two
semantically identical statements with different parameters look
distinct. The digest collapses them to a single shape, which makes a
handful of analyses trivial:

- **Detecting new query shapes.** Group your Parquet output by
  `query_hash` and `min(timestamp)` — any hash whose first appearance is
  recent is, by definition, a brand-new query shape introduced into the
  workload (a new code path, a feature rollout, a misbehaving ORM, an
  unexpected reporting query). This is the single highest-signal use of
  the field.
- **Per-shape volume / latency attribution.** `COUNT(*) GROUP BY
  query_hash` gives you the row-event volume contributed by each query
  shape; combined with the row-image deltas in `data` / `old`, you can
  see which shapes dominate writes.
- **Compact joins between observability sources.** `query_hash` matches
  what `performance_schema.events_statements_summary_by_digest.DIGEST`
  reports on the live server, so you can join Parquet-side analytics to
  live-server statement statistics without round-tripping through the
  raw SQL text.

### Where the JDBC URL points

It does not need to point at the *source* MySQL whose binlog you're
processing. `STATEMENT_DIGEST` is a deterministic function of the SQL
text and the parser version, so any reachable MySQL 5.7+ instance works
— a local dev container, a sidecar, an existing analytics MySQL,
whatever's nearest. We typically point it at a throwaway
`docker run --rm mysql:8.0 ...` on the same host as the converter.

### Performance note

Each `ROWS_QUERY` event triggers one round-trip to MySQL, so a binlog
with millions of distinct statements adds latency proportional to its
`ROWS_QUERY` count. If you only care about *which* shapes exist, not
about every individual occurrence, you can skip the flag and compute
digests in batch over `query` post hoc with the same SQL functions.

### When the column is null

- `--digest-mysql=...` was not provided.
- The query text could not be digested (the call is logged at WARN and
  fingerprint/hash stay null for that row).
- The source binlog was generated with `binlog_rows_query_log_events=OFF`,
  so there's no `query` text to digest in the first place.

---

## Output Parquet schema

Each output file is a single Parquet file (Snappy-compressed) whose schema is
flat. The column list is defined in
[`ParquetExporter.createExportableEventSchema`](src/main/java/io/github/binlog2parquet/ParquetExporter.java).

| Column              | Parquet type           | Required | Description                                                                                                |
| ------------------- | ---------------------- | -------- | ---------------------------------------------------------------------------------------------------------- |
| `binarylog`         | `string`               | optional | Source binlog filename (e.g. `mysql-bin.000042`).                                                          |
| `position`          | `int64`                | optional | Byte offset of the source event inside the binlog.                                                         |
| `gtid_position`     | `string`               | optional | Compact GTID position (e.g. `<uuid>:<txn>`).                                                               |
| `gtid`              | `string`               | optional | Full GTID string (`<uuid>:<txn>`).                                                                         |
| `gtid_uuid`         | `string`               | optional | Server-uuid component of the GTID.                                                                          |
| `gtid_txn`          | `int64`                | optional | Transaction-number component of the GTID.                                                                   |
| `server_id`         | `int64`                | required | Source MySQL `server_id`.                                                                                  |
| `timestamp`         | `int64`                | required | Event timestamp (millis since epoch).                                                                       |
| `timestamp_string`  | `int64` (TIMESTAMP_MILLIS, UTC) | required | Same timestamp, typed as a Parquet `TIMESTAMP_MILLIS`.                                              |
| `event`             | `string`               | optional | Event type (`EXT_WRITE_ROWS`, `EXT_UPDATE_ROWS`, `EXT_DELETE_ROWS`, `QUERY`, `XID`, etc.).                  |
| `table`             | `string`               | optional | Fully-qualified table name `<db>.<table>` for row events.                                                  |
| `query`             | `string`               | optional | Original SQL statement (from `ROWS_QUERY` events; requires `binlog_rows_query_log_events=ON`).             |
| `query_fingerprint` | `string`               | optional | Normalized statement form (literals stripped). Populated only when `--digest-mysql=...` is supplied; otherwise null. |
| `query_hash`        | `string`               | optional | Stable hash of the normalized statement. Populated only when `--digest-mysql=...` is supplied; otherwise null.       |
| `data`              | `string` (JSON)        | optional | Post-image of the row, as a JSON object keyed by column name.                                              |
| `old`               | `string` (JSON)        | optional | Pre-image of the row (UPDATE / DELETE only).                                                               |
| `changed`           | `string` (JSON)        | optional | UPDATE only: object of `{ "<col>": {"old": ..., "new": ...} }` for columns whose value actually changed.   |
| `columns`           | `string` (JSON)        | optional | UPDATE only: JSON array of the names of columns that changed.                                              |

### Per-event-type semantics

The Parquet file contains **only DML row events** — one Parquet row per
affected MySQL row. Everything else from the binlog (`GTID`, `XID`,
`QUERY`, `ROTATE`, `FORMAT_DESCRIPTION`, `PREVIOUS_GTIDS`, `TABLE_MAP`,
`ROWS_QUERY`) is consumed during parsing to populate the GTID / query /
table fields on the DML rows, but does **not** produce its own Parquet row.

| `event`                            | What's filled in                                                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `EXT_WRITE_ROWS` (INSERT)          | `data` = inserted row image. `old`/`changed` are unset.                                                             |
| `EXT_UPDATE_ROWS` (UPDATE)         | `data` = post-image, `old` = pre-image, `changed` = diff, `columns` = changed-column names.                         |
| `EXT_DELETE_ROWS` (DELETE)         | `old` = pre-image. `data` is the raw event-data string; `changed`/`columns` are unset.                              |

> **DDL is skipped.** `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, etc.
> arrive in the binlog as `QUERY` events. `binlog2parquet` does not emit
> Parquet rows for `QUERY` events, so DDL **never appears in the output**.
> If you need a record of schema changes, capture them out-of-band (for
> example by running `mysqlbinlog` on the same files and grepping for
> `ALTER`/`CREATE`/`DROP`).

### Caveats

- **One MySQL row event ≠ one Parquet row.** A single `WRITE_ROWS` /
  `UPDATE_ROWS` / `DELETE_ROWS` event in the binlog can carry multiple
  affected rows (this is normal — MySQL groups all rows touched by the same
  statement under one event header). `binlog2parquet` **expands** every
  such event into one Parquet row per affected MySQL row. As a result:
    - The same `(binarylog, position, gtid_position)` triple can appear on
      multiple consecutive Parquet rows.
    - The same `query` text can appear on every row of a multi-row INSERT.
    - Row order within a single source event matches the order in the
      binlog, but consumers should not rely on insertion-order in the
      Parquet file beyond what `(binarylog, position)` and a counter
      ordering provide.
  Downstream code that wants "one row per source statement" should
  re-aggregate by `(binarylog, position)` or by `gtid_position`.

- **GTID grouping.** All row events between a `GTID` event and the next
  `XID` (transaction commit) share a single `gtid_position`. Use that to
  reconstruct transactions.

- **JSON encoding.** `data`, `old`, `changed`, and `columns` are stored as
  JSON strings (Parquet logical type `JSON`), not as Parquet structs.
  Numeric and binary MySQL values may serialize as their Java
  `toString()` form; consumers should treat values defensively.

- **`NULL` columns.** A column whose value is NULL in the source row is
  emitted as JSON `null` inside the `data` / `old` JSON object, not as a
  missing key.

- **`query_fingerprint` / `query_hash`.** These fields are populated by
  asking a MySQL server for `STATEMENT_DIGEST(...)` /
  `STATEMENT_DIGEST_TEXT(...)`. They are filled in only when you pass
  `--digest-mysql=<jdbc-url>`; otherwise they are left null. See
  [Query fingerprinting](#query-fingerprinting-optional) for why this is
  useful.

- **Schema is not table-aware.** All events from all tables share the same
  flat schema. Column values are JSON-encoded inside `data` / `old`.
  Downstream queries typically extract specific columns with engine-native
  JSON functions (DuckDB `json_extract`, Spark `from_json`, Trino
  `json_extract_scalar`, etc.).

---

## Deployment

### 1. Building the runtime artifact

You can ship the tool as either a fat jar or a Docker image.

**Fat jar (requires JDK 11+ on the build host):**

```bash
./mvnw -DskipTests package
# Distribute target/binlog2parquet-0.1.0-all.jar wherever you need it.
```

<a id="docker-zero-host-install"></a>
**Docker (zero host install):**

The bundled [`Dockerfile`](Dockerfile) is multi-stage: stage 1 uses
`maven:3.9-eclipse-temurin-11` to build the shaded jar, stage 2 copies
that jar onto a slim `eclipse-temurin:11-jre` runtime. Everything that
the build needs — JDK, Maven, transitive dependency downloads — is
pulled into containers, so **the host needs only Docker**. No `java`,
`mvn`, `mysql`, or anything else has to be installed locally.

```bash
# 1. Build the image. This runs the entire Maven build inside a
#    container; nothing is installed on the host.
docker build -t binlog2parquet .

# 2. Convert a binlog. The image declares two well-known mount points:
#      /input   -> read-only, where binlogs are read from
#      /output  -> read/write, where parquet files are written
#    Both can be backed by local dirs or by S3 mounts on the host.
docker run --rm \
    -v "$PWD/test/samples:/input:ro" \
    -v "$PWD/out:/output" \
    -e JAVA_OPTS="-Xmx4g" \
    binlog2parquet \
    /input/01-basic-crud.binlog /output
```

Output lands in `./out/` on the host. Tune the JVM heap by overriding
`JAVA_OPTS` (e.g. `-e JAVA_OPTS="-Xmx32g"` for production-scale binlogs).

#### With query fingerprinting, also containerized

You don't need a host MySQL to populate `query_hash` /
`query_fingerprint` either — run a sidecar `mysql:8.0` on the host
network and point `--digest-mysql` at `127.0.0.1`:

```bash
# Throwaway MySQL just for STATEMENT_DIGEST() calls (host networking).
docker run -d --rm --name digest-mysql \
    --network host \
    -e MYSQL_ROOT_PASSWORD=rootpw \
    mysql:8.0

# Wait until ready, then run the converter on the host network too:
docker run --rm \
    --network host \
    -v "$PWD/test/samples:/input:ro" \
    -v "$PWD/out:/output" \
    -e JAVA_OPTS="-Xmx4g" \
    binlog2parquet \
    --digest-mysql="jdbc:mysql://127.0.0.1:3306/sys?user=root&password=rootpw&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
    /input/01-basic-crud.binlog /output

# When done:
docker rm -f digest-mysql
```

> `--network host` is Linux-only in the strict sense; on Docker Desktop
> for macOS / Windows, recent versions emulate it well enough for this
> use case. If you're on a host where host-mode isn't available, fall
> back to a user-defined bridge network (`docker network create ...`)
> and reference the sidecar by container name.

#### Image size & layer reuse

The runtime image carries only `eclipse-temurin:11-jre` plus the
~80 MB shaded jar; the Maven build cache and the JDK toolchain stay in
the build stage and never ship to production. Re-builds re-use Docker
layers, so an unchanged `pom.xml` skips the dependency-download step
on subsequent builds.

### 2. S3 input/output via filesystem mounts

This tool reads and writes ordinary files. To use S3-resident binlogs or to
land Parquet output back on S3, mount the bucket as a regular POSIX
filesystem on the host **before** invoking the tool — no AWS SDK is bundled
in the jar.

| Mount tool                                                                                      | Best for                                                  | Notes |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------- | ----- |
| [**S3 Files** (AWS-native, NFS-based)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-mounting.html) | EC2/EKS workloads on a dedicated S3 file system           | Official AWS service; works via `mount -t s3files`; auto-mountable in `/etc/fstab` |
| [Mountpoint for Amazon S3 (FUSE)](https://github.com/awslabs/mountpoint-s3)                     | Read-heavy / write-once workloads on plain S3 buckets     | Single binary; sequential writes only; ideal for binlog ingestion |
| [`s3fs-fuse`](https://github.com/s3fs-fuse/s3fs-fuse)                                           | Read/write access with POSIX-ish semantics                | Slower; broader compatibility |
| [`rclone mount`](https://rclone.org/commands/rclone_mount/)                                     | Multi-cloud, optional VFS caching                         | Mature, configurable |

> Both AWS-supported options (S3 Files and Mountpoint for Amazon S3)
> require an EC2 instance profile (or equivalent IAM role) with at
> minimum `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` on the
> target bucket(s). For S3 Files, also `s3:DeleteObject` if you expect
> the converter to overwrite output files.

#### Option A — S3 Files (the AWS-native NFS mount)

Best when you've already provisioned an S3 file system (`fs-...`) and
want a vanilla `mount -t ...` entry that survives reboots. Full canonical
docs: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-mounting.html>.

```bash
# 1. Install the helper package.
#    Amazon Linux 2023 / RHEL / Fedora:
sudo yum install -y amazon-efs-utils
#    Ubuntu / Debian:
sudo apt-get update && sudo apt-get install -y amazon-efs-utils

# 2. Mount your S3 file system (replace fs-id with your real one).
FS=fs-0123456789abcdef0
sudo mkdir -p /mnt/s3-binlogs
sudo mount -t s3files "$FS:/" /mnt/s3-binlogs

# 3. (Optional) make it persistent across reboots:
echo "$FS:/ /mnt/s3-binlogs s3files _netdev,nofail 0 0" | sudo tee -a /etc/fstab
sudo mount -a
```

Defaults pulled in by `mount -t s3files`: NFS 4.2, TLS 1.2 in transit,
IAM auth, 1 MB rsize/wsize, hard mounts. The instance must live in the
same Availability Zone as the file system's mount target, and security
groups have to permit NFS traffic between them.

You can then point `binlog2parquet` at the path exactly as if it were
local:

```bash
java -Xmx8g -jar target/binlog2parquet-0.1.0-all.jar \
    /mnt/s3-binlogs/mysql-bin.000042 \
    /mnt/s3-binlogs/parquet/
```

#### Option B — Mountpoint for Amazon S3 (FUSE, no file-system needed)

Best when you want to mount an existing **plain S3 bucket** without
provisioning anything new. A single user-space binary; no NFS, no AZ
constraints.

```bash
# Install mountpoint-s3 on Ubuntu.
wget https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb
sudo apt-get install -y ./mount-s3.deb

# Read-only mount for binlog inputs.
sudo mkdir -p /mnt/s3-binlogs
sudo mount-s3 my-binlog-bucket /mnt/s3-binlogs --read-only

# Read/write mount for Parquet output.
sudo mkdir -p /mnt/s3-parquet
sudo mount-s3 my-parquet-bucket /mnt/s3-parquet
```

> **Sequential-writes-only.** Mountpoint is optimized for "write a whole
> file once, in order" — exactly the access pattern Parquet writers
> produce. Random writes, in-place edits, and renames are not supported.
> If you need general POSIX semantics on an S3 bucket, use `s3fs-fuse`
> or `rclone mount` instead.

For an end-to-end EC2 walkthrough of Mountpoint — instance-profile
setup, performance observations, and gotchas — see the companion blog
post: <https://avaitla16.hashnode.dev/s3-files-first-impressions>.

### 3. Pulling binlogs from a live MySQL into the mount

`mysqlbinlog --read-from-remote-server --raw` already writes whole binlog
files to disk — point that "disk" at the S3 mountpoint and the file lands
in the bucket on close. No extra S3 client is required.

A minimal Python helper:

```python
import os
import subprocess
import logging
from typing import List

import pymysql
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class CompletedBinlogs(BaseModel):
    server_id: int
    current: str
    binlogs: List[str]


def get_completed_binlogs(host: str, user: str, password: str) -> CompletedBinlogs:
    """Return the list of rotated (i.e. complete) binlogs on the server."""
    logger.info("Connecting to %s as %s", host, user)
    conn = pymysql.connect(host=host, user=user, password=password)
    with conn.cursor() as cursor:
        cursor.execute("SHOW BINARY LOGS;")
        logs = [row[0] for row in cursor.fetchall()]

        cursor.execute("SHOW MASTER STATUS;")
        current = cursor.fetchone()[0]

        cursor.execute("SELECT @@SERVER_ID;")
        server_id = cursor.fetchone()[0]
    conn.close()

    completed = sorted(name for name in logs if name != current)
    return CompletedBinlogs(server_id=server_id, current=current, binlogs=completed)


def download_binlog_to_path(source_host: str, user: str, password: str,
                            log_name: str, dest_dir: str) -> str:
    """
    Download `log_name` from `source_host` into `dest_dir`.

    `dest_dir` can be a local path OR a path under an s3fs / mountpoint-s3 /
    rclone mount — once mysqlbinlog finishes writing, the file lands in the
    bucket.
    """
    os.makedirs(dest_dir, exist_ok=True)
    cmd = [
        "mysqlbinlog",
        "--read-from-remote-server",
        "--raw",
        "--result-file=" + dest_dir.rstrip("/") + "/",
        "-h", source_host,
        "-u", user,
        "-p" + password,
        log_name,
    ]
    logger.info("Running mysqlbinlog for %s", log_name)
    subprocess.run(cmd, check=True)
    return os.path.join(dest_dir, log_name)


if __name__ == "__main__":
    info = get_completed_binlogs("db.internal", "repl_user", os.environ["MYSQL_PASSWORD"])
    for log in info.binlogs:
        download_binlog_to_path(
            "db.internal", "repl_user", os.environ["MYSQL_PASSWORD"],
            log, "/mnt/s3-binlogs/incoming/",
        )
```

### 4. Putting it together

Once binlogs are landing under `/mnt/s3-binlogs/...` and you have an
output mount at `/mnt/s3-parquet/...`, the conversion is a single command:

```bash
java -Xmx8g -jar target/binlog2parquet-0.1.0-all.jar \
    /mnt/s3-binlogs/incoming/mysql-bin.000042 \
    /mnt/s3-parquet/
```

Or, the same thing in Docker:

```bash
docker run --rm \
    -v /mnt/s3-binlogs:/input:ro \
    -v /mnt/s3-parquet:/output \
    -e JAVA_OPTS="-Xmx8g" \
    binlog2parquet \
    /input/incoming/mysql-bin.000042 /output
```

A typical EC2 deployment ends up looking like:

```
              +------------------------+
              |  Source MySQL          |
              |  (binlog ROW + FULL)   |
              +-----------+------------+
                          |
                          | mysqlbinlog --read-from-remote-server --raw
                          v
+-------------------------+--------------------------+
| EC2 instance (instance-profile w/ S3 access)        |
|                                                     |
|  /mnt/s3-binlogs   <- mountpoint-s3 (read-only)     |
|  /mnt/s3-parquet   <- mountpoint-s3 (read/write)    |
|                                                     |
|  java -Xmx8g -jar binlog2parquet-0.1.0-all.jar \    |
|       /mnt/s3-binlogs/.../mysql-bin.000042 \        |
|       /mnt/s3-parquet/                              |
+-----------------------------------------------------+
                          |
                          v
                +---------+---------+
                |  S3: parquet/...  |
                +-------------------+
```

---

## Memory monitoring

`MemoryMonitor` runs throughout each invocation and emits periodic heap
reports to stderr, e.g.:

```
[MemoryMonitor] PROCESS_START - Heap: 248MB used / 2048MB committed / 2048MB max (12.1% used, 1800MB free) | Non-Heap: 64MB used / 96MB committed
[MemoryMonitor] PRE_PARQUET_WRITE - Heap: 1742MB used / 2048MB committed / 2048MB max (85.0% used, 306MB free) | Non-Heap: 88MB used / 96MB committed
```

If the tool crashes with `OutOfMemoryError`, raise `-Xmx` and re-run. As a
rough rule of thumb on `binlog_row_metadata=FULL` workloads, plan for
**heap ≈ 8–10× the on-disk binlog size**; wide tables and long
transactions push that ratio higher.

In production, we have seen single binlog files require **upwards of
150 GB** of heap to convert — chiefly because `binlog_row_metadata=FULL`
embeds a full copy of the table's column metadata in every row event, and
the connector materializes those events in memory before they are
serialized to Parquet. **Always size your JVM to your worst-observed
workload**, not your average one, and provision the host accordingly. A
crashed conversion costs a full re-run.

---

## Test data

The repository ships with a small test rig for generating reproducible
binlog samples. See [`test/`](test/):

- `test/scenarios/*.sql` — self-contained scenarios (basic CRUD, multi-row
  transactions, mixed column types).
- `test/generate-binlogs.sh` — Docker-backed harness that boots a MySQL 8
  container with the required settings, replays every scenario, rotates
  the log between scenarios, and copies the resulting binlog files into
  `test/samples/` named after the scenario that produced them
  (`01-basic-crud.binlog`, `02-multi-statement-txn.binlog`,
  `03-mixed-types.binlog`). Bootstrap output from the MySQL container is
  intentionally discarded so the exported files only contain scenario DML.
- `test/samples/` — pre-generated binlog samples checked into the repo
  (a few KB each; regenerate with `make seed-tests`).

Regenerate samples:

```bash
make seed-tests   # or: ./test/generate-binlogs.sh
```

Then process one through the tool:

```bash
make run INPUT=test/samples/02-multi-statement-txn.binlog OUTPUT=/tmp/out
```

---

## License

This project is licensed under the **Apache License, Version 2.0** —
see [`LICENSE`](LICENSE) for the full text, or
<http://www.apache.org/licenses/LICENSE-2.0> for the canonical copy.

The
[`mysql-binlog-connector-java`](https://github.com/osheroff/mysql-binlog-connector-java)
library this project depends on is also distributed under Apache 2.0.
