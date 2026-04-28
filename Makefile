.PHONY: help build run seed-tests clean

JAR := target/binlog2parquet-0.1.0-all.jar
INPUT ?=
OUTPUT ?=
JAVA_OPTS ?= -Xmx4g
DIGEST_MYSQL ?=

help:
	@echo "Targets:"
	@echo "  build       Compile the shaded uber-jar (./mvnw -DskipTests package)."
	@echo "  run         Convert one binlog -> Parquet."
	@echo "                Required: INPUT=<binlog> OUTPUT=<file-or-dir>"
	@echo "                Optional: JAVA_OPTS=-Xmx8g  DIGEST_MYSQL=<jdbc-url>"
	@echo "  seed-tests  Regenerate test/samples/ via test/generate-binlogs.sh"
	@echo "                (boots a throwaway MySQL 8 container and replays scenarios)."
	@echo "  clean       Remove target/."

build:
	./mvnw -DskipTests package

$(JAR):
	$(MAKE) build

run: $(JAR)
	@if [ -z "$(INPUT)" ] || [ -z "$(OUTPUT)" ]; then \
	    echo "ERROR: INPUT and OUTPUT are required."; \
	    echo "  make run INPUT=test/samples/01-basic-crud.binlog OUTPUT=/tmp/out"; \
	    exit 2; \
	fi
	java $(JAVA_OPTS) -jar $(JAR) \
	    $(if $(DIGEST_MYSQL),--digest-mysql="$(DIGEST_MYSQL)",) \
	    "$(INPUT)" "$(OUTPUT)"

seed-tests:
	./test/generate-binlogs.sh

clean:
	rm -rf target
