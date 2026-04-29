#!/usr/bin/env bash
#
# binlog2parquet — interactive launcher.
#
# Walks the user through the three required choices and runs the
# converter accordingly:
#
#   1. Input binlog file
#   2. Output shape — one Parquet file per source table (a directory)
#                     OR every event merged into a single Parquet file
#   3. Output path
#
# Single-file mode runs the converter into a staging directory (the
# Java tool always splits per table) and then merges the per-table
# Parquets into one file via DuckDB. Directory mode invokes the
# converter directly. Either way, the JVM-side code path is the same.
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="${ROOT_DIR}/target/binlog2parquet-0.1.0-all.jar"
JAVA_OPTS="${JAVA_OPTS:--Xmx4g}"

ensure_jar() {
    if [[ ! -f "$JAR" ]]; then
        echo "[tui] Shaded jar not found at $JAR — building..."
        (cd "$ROOT_DIR" && ./mvnw -DskipTests package)
    fi
}

prompt_input() {
    local default_dir="${ROOT_DIR}/test/samples"
    echo
    echo "Step 1/3 — Input binlog"
    if [[ -d "$default_dir" ]]; then
        local samples
        samples=$(ls -1 "$default_dir" 2>/dev/null || true)
        if [[ -n "$samples" ]]; then
            echo "  Suggested samples (relative to $default_dir):"
            printf '    %s\n' $samples
        fi
    fi
    while true; do
        read -e -r -p "  Path to input binlog: " input
        input="${input/#~/$HOME}"
        if [[ -z "$input" ]]; then
            echo "  (empty — please enter a path)"
            continue
        fi
        if [[ ! -f "$input" ]]; then
            # Try resolving against test/samples as a convenience.
            if [[ -f "${default_dir}/${input}" ]]; then
                input="${default_dir}/${input}"
            else
                echo "  '$input' is not a file. Try again."
                continue
            fi
        fi
        INPUT_PATH="$input"
        break
    done
}

prompt_mode() {
    echo
    echo "Step 2/3 — Output shape"
    echo "  [1] Directory  — one Parquet file per source table"
    echo "                   (default; better Parquet compression — see README)"
    echo "  [2] Single file — every event merged into one Parquet"
    echo "                   (requires duckdb on PATH; runs an extra merge step)"
    while true; do
        read -r -p "  Choice [1/2, default 1]: " choice
        choice="${choice:-1}"
        case "$choice" in
            1) MODE="directory"; break ;;
            2) MODE="single"; break ;;
            *) echo "  Enter 1 or 2." ;;
        esac
    done
}

prompt_output() {
    local base default
    base="$(basename "${INPUT_PATH%.*}")"
    echo
    echo "Step 3/3 — Output path"
    if [[ "$MODE" == "directory" ]]; then
        echo "  Will create / clean a directory and write one .parquet per source table."
        default="/tmp/binlog2parquet/${base}"
    else
        echo "  Will write a single .parquet file at this path."
        default="/tmp/binlog2parquet/${base}.parquet"
    fi
    read -e -r -p "  Output path [$default]: " output
    output="${output:-$default}"
    output="${output/#~/$HOME}"
    OUTPUT_PATH="$output"
}

run_directory_mode() {
    echo
    echo "[tui] Running converter (directory mode)"
    echo "      INPUT  = $INPUT_PATH"
    echo "      OUTPUT = $OUTPUT_PATH"
    echo "      JAVA_OPTS = $JAVA_OPTS"
    echo
    java $JAVA_OPTS -jar "$JAR" "$INPUT_PATH" "$OUTPUT_PATH"
    echo
    echo "[tui] Done. Per-table files:"
    ls -1 "$OUTPUT_PATH" 2>/dev/null | sed 's/^/    /' || true
}

run_single_mode() {
    if ! command -v duckdb >/dev/null 2>&1; then
        echo
        echo "[tui] ERROR: single-file mode merges per-table Parquets via duckdb,"
        echo "      but 'duckdb' is not on PATH. Install it (brew install duckdb /"
        echo "      apt-get install duckdb / single-binary download) or pick"
        echo "      directory mode instead."
        exit 2
    fi

    local staging staging_out
    staging="$(mktemp -d -t binlog2parquet-staging.XXXXXX)"
    # shellcheck disable=SC2064
    trap "rm -rf '$staging'" EXIT
    # Pass a non-existing sub-path to the converter so it writes the
    # per-table files directly there. If we passed `$staging` (which
    # exists), the converter auto-generates an inner directory name
    # and our glob would miss the files.
    staging_out="${staging}/out"

    echo
    echo "[tui] Running converter (single-file mode, via DuckDB merge)"
    echo "      INPUT   = $INPUT_PATH"
    echo "      STAGING = $staging_out"
    echo "      OUTPUT  = $OUTPUT_PATH"
    echo "      JAVA_OPTS = $JAVA_OPTS"
    echo
    java $JAVA_OPTS -jar "$JAR" "$INPUT_PATH" "$staging_out"

    mkdir -p "$(dirname "$OUTPUT_PATH")"
    echo
    echo "[tui] Merging per-table Parquets into a single file via duckdb..."
    duckdb -c "
      COPY (
        SELECT * FROM read_parquet('${staging_out}/*.parquet')
        ORDER BY position
      ) TO '${OUTPUT_PATH}' (FORMAT 'parquet', CODEC 'snappy');
    "
    if [[ -f "$OUTPUT_PATH" ]]; then
        echo "[tui] Done. Wrote $(du -h "$OUTPUT_PATH" | awk '{print $1}') to $OUTPUT_PATH"
    else
        echo "[tui] ERROR: expected output file not created at $OUTPUT_PATH"
        exit 3
    fi
}

main() {
    echo "binlog2parquet — interactive launcher"
    echo "(Ctrl-C at any prompt to exit.)"
    ensure_jar
    prompt_input
    prompt_mode
    prompt_output
    if [[ "$MODE" == "directory" ]]; then
        run_directory_mode
    else
        run_single_mode
    fi
}

main "$@"
