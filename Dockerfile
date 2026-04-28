# Build stage: produce the shaded uber-jar.
# JDK 21 (latest LTS) for the build; pom.xml targets bytecode 11, so the
# resulting jar still runs on any JRE >= 11.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests package

# Runtime stage: minimal JRE + the uber-jar. Defaults to JRE 21; switch this
# tag to `11-jre` / `17-jre` if you specifically need an older runtime.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/binlog2parquet-*-all.jar /app/binlog2parquet.jar

# Default JVM heap; override at runtime: `docker run -e JAVA_OPTS=-Xmx4g ...`
ENV JAVA_OPTS="-Xmx2g"

# Conventional mount points: bring binlogs in via /input, write parquet to /output.
# Both can be backed by an S3 mount on the host (s3fs / mountpoint-s3 / rclone).
VOLUME ["/input", "/output"]

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/binlog2parquet.jar \"$@\"", "--"]
CMD ["--help"]
