package io.github.binlog2parquet;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.PreviousGtidSetEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static String mergeGtids(List<GtidEventData> gtids) {
        if (gtids == null || gtids.isEmpty()) {
            return "";
        }

        Map<String, List<Long>> gtidsByServer = new HashMap<>();
        for (GtidEventData gtid : gtids) {
            String serverUuid = gtid.getMySqlGtid().getServerId().toString();
            long transactionNumber = gtid.getMySqlGtid().getTransactionId();
            gtidsByServer.computeIfAbsent(serverUuid, k -> new ArrayList<>()).add(transactionNumber);
        }

        List<String> serverGtidStrings = new ArrayList<>();
        for (Map.Entry<String, List<Long>> entry : gtidsByServer.entrySet()) {
            List<Long> txns = entry.getValue();
            Collections.sort(txns);

            List<String> ranges = new ArrayList<>();
            long start = txns.get(0);
            long end = start;
            for (int i = 1; i < txns.size(); i++) {
                long current = txns.get(i);
                if (current == end + 1) {
                    end = current;
                } else {
                    ranges.add(start == end ? String.valueOf(start) : (start + "-" + end));
                    start = current;
                    end = current;
                }
            }
            ranges.add(start == end ? String.valueOf(start) : (start + "-" + end));

            serverGtidStrings.add(entry.getKey() + ":" + String.join(":", ranges));
        }

        Collections.sort(serverGtidStrings);
        return String.join(",", serverGtidStrings);
    }

    // Pads the trailing numeric segment of a binlog filename to 10 digits so
    // that lexicographic ordering (S3 ListObjects, `ls`, glob expansion) matches
    // chronological order. Third-party ETL systems that key off filename order
    // for incremental S3 ingest (e.g. ClickPipes) rely on this — without
    // padding, `mysql-bin.10` sorts before `mysql-bin.2` and ingest skips files.
    // The full generated filename also embeds <serverId> (see
    // generateOutputFilename) so that binlogs from multiple MySQL servers — or
    // from successive primaries in an HA failover — can be poured into one
    // output directory / S3 prefix without filename collisions.
    static String extractAndPadNumber(String filename) {
        int lastDotIndex = filename.lastIndexOf('.');
        if (lastDotIndex != -1 && lastDotIndex < filename.length() - 1) {
            String numberPart = filename.substring(lastDotIndex + 1);
            try {
                return String.format("%010d", Long.parseLong(numberPart));
            } catch (NumberFormatException e) {
                return "0000000000";
            }
        }
        return "0000000000";
    }

    static String generateOutputFilename(String inputFilename, long serverId) {
        String paddedNumber = extractAndPadNumber(inputFilename);
        int lastDotIndex = inputFilename.lastIndexOf('.');
        String baseName = lastDotIndex != -1 ? inputFilename.substring(0, lastDotIndex) : inputFilename;
        return baseName + "." + serverId + "." + paddedNumber;
    }

    public static String processFile(String inputPath, String outputPath, boolean isDirectory) throws IOException {
        return processFile(inputPath, outputPath, isDirectory, null);
    }

    public static String processFile(String inputPath, String outputPath, boolean isDirectory,
                                     String digestMysqlUrl) throws IOException {
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            throw new IOException("Input binlog file does not exist: " + inputPath);
        }

        if (!isDirectory) {
            File parent = new File(outputPath).getAbsoluteFile().getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IOException("Failed to create parent directory: " + parent);
            }
        }

        String fname = inputFile.getName();
        ProcessingStats stats = new ProcessingStats();
        stats.setFileSizeBytes(inputFile.length());

        MemoryMonitor.startPeriodicMonitoring(30);
        MemoryMonitor.logMemoryUsage("PROCESS_START");

        Connection digestConn = null;
        PreparedStatement digestStmt = null;
        if (digestMysqlUrl != null) {
            try {
                digestConn = DriverManager.getConnection(digestMysqlUrl);
                digestStmt = digestConn.prepareStatement(
                    "SELECT STATEMENT_DIGEST(?), STATEMENT_DIGEST_TEXT(?)");
                logger.info("Query fingerprinting enabled (using STATEMENT_DIGEST against MySQL).");
            } catch (SQLException e) {
                throw new IOException("Failed to connect to digest MySQL: " + e.getMessage(), e);
            }
        }

        try {

        long fileScanningStart = System.currentTimeMillis();

        List<EventWrapper> eventsWrapped = new ArrayList<>();
        long serverId = 0;
        Long earliestTimestamp = null;
        Long latestTimestamp = null;

        // First pass: extract metadata (server id, gtids, timestamps).
        String prevGtids = null;
        String nextBinlog = null;
        List<GtidEventData> gtids = new ArrayList<>();

        try (BinaryLogFileReader reader = new BinaryLogFileReader(inputFile)) {
            Event event;
            while ((event = reader.readEvent()) != null) {
                EventType eventType = event.getHeader().getEventType();

                if (!isExpectedEventType(eventType)) {
                    logger.info("Event: {}", event);
                    throw new RuntimeException("Unexpected event type: " + eventType);
                }

                if (eventType == EventType.FORMAT_DESCRIPTION) {
                    earliestTimestamp = event.getHeader().getTimestamp();
                } else if (eventType == EventType.PREVIOUS_GTIDS) {
                    prevGtids = ((PreviousGtidSetEventData) event.getData()).getGtidSet();
                    serverId = event.getHeader().getServerId();
                } else if (eventType == EventType.ROTATE) {
                    nextBinlog = ((RotateEventData) event.getData()).getBinlogFilename();
                    latestTimestamp = event.getHeader().getTimestamp();
                } else if (eventType == EventType.GTID) {
                    gtids.add((GtidEventData) event.getData());
                }
            }
        }

        String mergedGtids = mergeGtids(gtids);
        String earliestFormatted = formatTimestamp(earliestTimestamp);
        String latestFormatted = formatTimestamp(latestTimestamp);

        logger.info("Server    ID: {}", serverId);
        logger.info("Binary   Log: {}", fname);
        logger.info("Prior  GTIDs: {}", prevGtids);
        logger.info("Merged GTIDs: {}", mergedGtids);
        logger.info("Next     Log: {}", nextBinlog);
        logger.info("Earliest UTC: {}", earliestFormatted);
        logger.info("Latest   UTC: {}", latestFormatted);

        stats.setMetadata(serverId, fname, prevGtids, mergedGtids, nextBinlog,
                          earliestTimestamp, latestTimestamp, earliestFormatted, latestFormatted);

        // Second pass: build EventWrappers.
        long startPosition = 4;
        EventWrapper lastGtidEventWrapper = null;
        Map<Long, EventWrapper> tableMapEventWrapper = new HashMap<>();
        EventWrapper lastRowsQueryEventWrapper = null;
        List<EventWrapper> gtidCapturedEvents = new ArrayList<>();

        try (BinaryLogFileReader reader = new BinaryLogFileReader(inputFile)) {
            Event event;
            while ((event = reader.readEvent()) != null) {
                EventType eventType = event.getHeader().getEventType();
                stats.incrementEventType(eventType.toString());

                EventWrapper evWrapper;
                if (eventType == EventType.EXT_UPDATE_ROWS ||
                    eventType == EventType.EXT_WRITE_ROWS ||
                    eventType == EventType.EXT_DELETE_ROWS) {
                    evWrapper = new EventWrapper(event, startPosition, fname, lastGtidEventWrapper,
                                                 tableMapEventWrapper, lastRowsQueryEventWrapper,
                                                 event.getHeader().getServerId());
                    gtidCapturedEvents.add(evWrapper);
                } else {
                    evWrapper = new EventWrapper(event, startPosition, fname, lastGtidEventWrapper,
                                                 event.getHeader().getServerId());
                }

                if (eventType == EventType.GTID || eventType == EventType.MARIADB_GTID) {
                    lastGtidEventWrapper = evWrapper;
                    gtidCapturedEvents = new ArrayList<>();
                    tableMapEventWrapper = new HashMap<>();
                }

                if (eventType == EventType.TABLE_MAP) {
                    TableMapEventData mp = (TableMapEventData) event.getData();
                    tableMapEventWrapper.put(mp.getTableId(), evWrapper);
                }

                if (eventType == EventType.ROWS_QUERY) {
                    lastRowsQueryEventWrapper = evWrapper;
                    if (digestStmt != null) {
                        String q = ((RowsQueryEventData) event.getData()).getQuery();
                        EventWrapper.StatementDigestResult dr = computeDigest(digestStmt, q);
                        if (dr != null) {
                            evWrapper.setDigest(dr);
                        }
                    }
                }

                if (eventType == EventType.XID && lastGtidEventWrapper != null) {
                    lastGtidEventWrapper.gtidEventsWrapper = gtidCapturedEvents;
                    lastGtidEventWrapper = null;
                }

                eventsWrapped.add(evWrapper);
                startPosition = ((EventHeaderV4) event.getHeader()).getNextPosition();
            }
        }

        stats.setFileScanningTime(System.currentTimeMillis() - fileScanningStart);

        long parquetGenerationStart = System.currentTimeMillis();

        List<ExportableEvent> exportableEvents = new ArrayList<>();
        long globalCounter = 1;
        for (EventWrapper ev : eventsWrapped) {
            List<ExportableEvent> expEvents = ev.export(globalCounter);
            if (expEvents != null && !expEvents.isEmpty()) {
                globalCounter += expEvents.size();
                exportableEvents.addAll(expEvents);
            }
        }

        String finalOutputPath;
        if (isDirectory) {
            String outputFilename = generateOutputFilename(fname, serverId);
            finalOutputPath = Paths.get(outputPath, outputFilename).toString();
        } else {
            finalOutputPath = outputPath;
        }

        logger.info("Exporting {} events to: {}", exportableEvents.size(), finalOutputPath);
        MemoryMonitor.logMemoryUsage("PRE_PARQUET_WRITE");
        ParquetExporter.export(finalOutputPath, exportableEvents, stats);

        stats.setParquetGenerationTime(System.currentTimeMillis() - parquetGenerationStart);
        MemoryMonitor.logMemoryUsage("PROCESS_END");
        MemoryMonitor.stopPeriodicMonitoring();
        logProcessingStatistics(stats);

        return finalOutputPath;
        } finally {
            if (digestStmt != null) {
                try { digestStmt.close(); } catch (SQLException e) { /* ignore */ }
            }
            if (digestConn != null) {
                try { digestConn.close(); } catch (SQLException e) { /* ignore */ }
            }
        }
    }

    private static EventWrapper.StatementDigestResult computeDigest(PreparedStatement stmt, String query) {
        try {
            stmt.setString(1, query);
            stmt.setString(2, query);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new EventWrapper.StatementDigestResult(query, rs.getString(1), rs.getString(2));
                }
            }
        } catch (SQLException e) {
            logger.warn("STATEMENT_DIGEST failed for query (length={}): {}", query.length(), e.getMessage());
        }
        return null;
    }

    private static boolean isExpectedEventType(EventType eventType) {
        switch (eventType) {
            case FORMAT_DESCRIPTION:
            case PREVIOUS_GTIDS:
            case GTID:
            case QUERY:
            case ROWS_QUERY:
            case TABLE_MAP:
            case EXT_UPDATE_ROWS:
            case EXT_WRITE_ROWS:
            case EXT_DELETE_ROWS:
            case ROTATE:
            case XID:
                return true;
            default:
                return false;
        }
    }

    private static String formatTimestamp(Long timestampMs) {
        if (timestampMs == null) {
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return Instant.ofEpochMilli(timestampMs).atZone(ZoneOffset.UTC).format(formatter);
    }

    private static void logProcessingStatistics(ProcessingStats stats) {
        logger.info("=====================================");
        logger.info("         PROCESSING STATISTICS       ");
        logger.info("=====================================");
        logger.info("Total Events: {}", stats.getTotalEvents());

        Map<String, Long> eventCounts = stats.getEventTypeCounts();
        if (!eventCounts.isEmpty()) {
            logger.info("");
            logger.info("Event Types:");
            eventCounts.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                .forEach(entry -> logger.info("  {}: {}", entry.getKey(), entry.getValue()));
        }

        Map<String, Map<String, Long>> tableStats = stats.getTableStats();
        if (!tableStats.isEmpty()) {
            logger.info("");
            logger.info("Table Operations:");
            tableStats.entrySet().stream()
                .sorted((e1, e2) -> {
                    long t1 = e1.getValue().values().stream().mapToLong(Long::longValue).sum();
                    long t2 = e2.getValue().values().stream().mapToLong(Long::longValue).sum();
                    return Long.compare(t2, t1);
                })
                .forEach(entry -> {
                    Map<String, Long> ops = entry.getValue();
                    logger.info("  {}: writes={}, updates={}, deletes={}, total={}",
                        entry.getKey(),
                        ops.getOrDefault("writes", 0L),
                        ops.getOrDefault("updates", 0L),
                        ops.getOrDefault("deletes", 0L),
                        ops.getOrDefault("total", 0L));
                });
        }
        logger.info("=====================================");
    }

    private static void printUsage() {
        System.out.println("Usage: binlog2parquet [--digest-mysql=<jdbc-url>] <input-binlog-file> <output-path>");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("  <input-binlog-file>      Path to a MySQL binary log file.");
        System.out.println("  <output-path>            Output path. If it points to an existing");
        System.out.println("                           directory, an output filename of the form");
        System.out.println("                           <basename>.<serverId>.<paddedSeq> is generated");
        System.out.println("                           inside it (the zero-padded sequence keeps");
        System.out.println("                           lexicographic order = chronological order, so");
        System.out.println("                           S3-keyed incremental ETL like ClickPipes can");
        System.out.println("                           ingest in source order). Otherwise it is used");
        System.out.println("                           verbatim and parent directories are created.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --digest-mysql=<jdbc-url>");
        System.out.println("                           Optional. JDBC URL of any MySQL 5.7+ instance");
        System.out.println("                           (the source server, a local dev MySQL, anything).");
        System.out.println("                           When provided, every ROWS_QUERY is run through");
        System.out.println("                           STATEMENT_DIGEST() / STATEMENT_DIGEST_TEXT() and");
        System.out.println("                           the resulting hash + normalized text are written");
        System.out.println("                           to the query_hash / query_fingerprint columns.");
        System.out.println("                           Without this flag, those columns are null.");
        System.out.println();
        System.out.println("                           Example:");
        System.out.println("                             --digest-mysql=\"jdbc:mysql://127.0.0.1:3306/sys?\\");
        System.out.println("                             user=root&password=secret&useSSL=false&allowPublicKeyRetrieval=true\"");
        System.out.println();
        System.out.println("S3 inputs/outputs are supported via filesystem mounts (s3fs, rclone, mountpoint-s3).");
    }

    public static void main(String[] args) throws IOException {
        String digestMysqlUrl = null;
        List<String> positional = new ArrayList<>();

        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                printUsage();
                return;
            } else if (arg.startsWith("--digest-mysql=")) {
                digestMysqlUrl = arg.substring("--digest-mysql=".length());
            } else if (arg.startsWith("--")) {
                System.err.println("Unknown flag: " + arg);
                printUsage();
                System.exit(1);
            } else {
                positional.add(arg);
            }
        }

        if (positional.size() != 2) {
            printUsage();
            System.exit(1);
        }

        String inputPath = positional.get(0);
        String outputPath = positional.get(1);
        boolean isDirectory = new File(outputPath).isDirectory();

        processFile(inputPath, outputPath, isDirectory, digestMysqlUrl);
    }
}
