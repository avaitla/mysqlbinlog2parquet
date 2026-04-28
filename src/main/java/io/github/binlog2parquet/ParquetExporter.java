package io.github.binlog2parquet;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

public class ParquetExporter {

    // Simple OutputFile implementation that doesn't use Hadoop filesystem
    private static class SimpleOutputFile implements OutputFile {
        private final String path;

        public SimpleOutputFile(String path) {
            this.path = path;
            System.out.println("[ParquetExporter] SimpleOutputFile created for path: " + path);
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            System.out.println("[ParquetExporter] Creating PositionOutputStream for: " + path + " (blockSizeHint: " + blockSizeHint + ")");
            ensureParentDirectoryExists();
            FileOutputStream fos = new FileOutputStream(path);
            System.out.println("[ParquetExporter] FileOutputStream created successfully for: " + path);
            return new SimplePositionOutputStream(fos);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            System.out.println("[ParquetExporter] Creating or overwriting PositionOutputStream for: " + path + " (blockSizeHint: " + blockSizeHint + ")");
            ensureParentDirectoryExists();
            FileOutputStream fos = new FileOutputStream(path);
            System.out.println("[ParquetExporter] FileOutputStream created/overwritten successfully for: " + path);
            return new SimplePositionOutputStream(fos);
        }

        private void ensureParentDirectoryExists() throws IOException {
            java.io.File outputFile = new java.io.File(path);
            java.io.File parentDir = outputFile.getParentFile();
            System.out.println("[ParquetExporter] Checking parent directory for: " + path);
            if (parentDir != null) {
                System.out.println("[ParquetExporter] Parent directory: " + parentDir.getAbsolutePath() + ", exists: " + parentDir.exists());
                if (!parentDir.exists()) {
                    System.out.println("[ParquetExporter] Creating parent directory: " + parentDir.getAbsolutePath());
                    if (!parentDir.mkdirs()) {
                        throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
                    }
                    System.out.println("[ParquetExporter] Parent directory created successfully: " + parentDir.getAbsolutePath());
                }
            } else {
                System.out.println("[ParquetExporter] No parent directory needed for: " + path);
            }
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 128 * 1024 * 1024; // 128MB
        }
    }

    private static class SimplePositionOutputStream extends PositionOutputStream {
        private final FileOutputStream fos;
        private long position = 0;

        public SimplePositionOutputStream(FileOutputStream fos) {
            this.fos = fos;
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            fos.write(b);
            position++;
            if (position % 1024 == 0) { // Log every 1KB written
                System.out.println("[ParquetExporter] Wrote " + position + " bytes so far");
            }
        }

        @Override
        public void write(byte[] b) throws IOException {
            fos.write(b);
            position += b.length;
            if (b.length > 1024) { // Log large writes
                System.out.println("[ParquetExporter] Large write: " + b.length + " bytes, total position: " + position);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            fos.write(b, off, len);
            position += len;
            if (len > 1024) { // Log large writes
                System.out.println("[ParquetExporter] Large write (offset): " + len + " bytes, total position: " + position);
            }
        }

        @Override
        public void flush() throws IOException {
            System.out.println("[ParquetExporter] Flushing stream, current position: " + position);
            fos.flush();
            System.out.println("[ParquetExporter] Stream flushed successfully");
        }

        @Override
        public void close() throws IOException {
            System.out.println("[ParquetExporter] Closing stream, final position: " + position);
            fos.close();
            System.out.println("[ParquetExporter] Stream closed successfully, wrote " + position + " bytes total");
        }
    }

    private static Configuration getParquetConfig() {
        Configuration conf = new Configuration();
        // Increase memory allocation for Parquet writer
        conf.setLong("parquet.writer.max-padding", 8 * 1024 * 1024); // 8MB
        conf.setFloat("parquet.memory.pool.ratio", 0.95f); // Use 95% of available memory
        conf.setLong("parquet.memory.min.chunk.size", 1024 * 1024); // 1MB minimum chunk
        // Suppress memory warnings by increasing thresholds
        conf.setFloat("parquet.writer.memory.threshold.check.ratio", 0.99f); // Only warn at 99%
        conf.setBoolean("parquet.writer.memory.aggressive.checking.enabled", false);
        // Set logging level to reduce noise
        conf.set("parquet.log.level", "ERROR");
        return conf;
    }

    public static MessageType createExportableEventSchema() {
        return Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("binarylog")
            .optional(PrimitiveType.PrimitiveTypeName.INT64).named("position")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("gtid_position")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("gtid")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("gtid_uuid")
            .optional(PrimitiveType.PrimitiveTypeName.INT64).named("gtid_txn")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("server_id")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("timestamp")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("timestamp_string")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("event")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("table")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("query")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("query_fingerprint")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("query_hash")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.jsonType()).named("data")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.jsonType()).named("old")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.jsonType()).named("changed")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.jsonType()).named("columns")
            .named("ExportableEvent");
    }

    private static Group createRecord(MessageType schema, ExportableEvent event) {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup();

        if (event.getBinarylog() != null) {
            group.append("binarylog", Binary.fromString(event.getBinarylog()));
        }
        Long position = parsePositionAsLong(event.getPosition());
        if (position != null) {
            group.append("position", position);
        }
        if (event.getGtidPosition() != null) {
            group.append("gtid_position", Binary.fromString(event.getGtidPosition()));
        }
        if (event.getGtid() != null) {
            group.append("gtid", Binary.fromString(event.getGtid()));
        }
        if (event.getGtidUuid() != null) {
            group.append("gtid_uuid", Binary.fromString(event.getGtidUuid()));
        }
        Long gtidTxn = parseGtidTxnAsLong(event.getGtidTxn());
        if (gtidTxn != null) {
            group.append("gtid_txn", gtidTxn);
        }
        group.append("server_id", event.getServerId());
        group.append("timestamp", event.getTimestamp());
        group.append("timestamp_string", event.getTimestamp());
        if (event.getEvent() != null) {
            group.append("event", Binary.fromString(event.getEvent()));
        }
        if (event.getTable() != null) {
            group.append("table", Binary.fromString(event.getTable()));
        }
        if (event.getQuery() != null) {
            group.append("query", Binary.fromString(event.getQuery()));
        }
        if (event.getQueryFingerprint() != null) {
            group.append("query_fingerprint", Binary.fromString(event.getQueryFingerprint()));
        }
        if (event.getQueryHash() != null) {
            group.append("query_hash", Binary.fromString(event.getQueryHash()));
        }
        if (event.getData() != null) {
            group.append("data", Binary.fromString(event.getData()));
        }
        if (event.getOld() != null) {
            group.append("old", Binary.fromString(event.getOld()));
        }
        if (event.getChanged() != null) {
            group.append("changed", Binary.fromString(event.getChanged()));
        }
        if (event.getColumns() != null) {
            group.append("columns", Binary.fromString(event.getColumns()));
        }
        return group;
    }

    private static Map<String, List<ExportableEvent>> groupEventsByTable(List<ExportableEvent> exportableEvents) {
        Map<String, List<ExportableEvent>> eventsByTable = new HashMap<>();
        
        for (ExportableEvent event : exportableEvents) {
            String tableName = event.getTable();
            
            // Handle null or empty table names
            if (tableName == null || tableName.trim().isEmpty()) {
                tableName = "unknown_table";
            } else {
                // Sanitize table name for use as filename
                tableName = sanitizeTableName(tableName);
            }
            
            eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(event);
        }
        
        return eventsByTable;
    }
    
    private static String sanitizeTableName(String tableName) {
        // Replace characters that are invalid for filenames
        return tableName.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
    
    private static void cleanDirectory(java.io.File directory) throws IOException {
        if (directory.exists() && directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    if (file.isDirectory()) {
                        cleanDirectory(file); // Recursively clean subdirectories
                        if (!file.delete()) {
                            throw new IOException("Failed to delete directory: " + file.getAbsolutePath());
                        }
                    } else {
                        if (!file.delete()) {
                            throw new IOException("Failed to delete file: " + file.getAbsolutePath());
                        }
                    }
                }
                System.out.println("[ParquetExporter] Cleaned " + (files.length > 0 ? files.length : "0") + " items from directory: " + directory.getAbsolutePath());
            }
        }
    }

    public static void export(String outputPath, List<ExportableEvent> exportableEvents) throws IOException {
        export(outputPath, exportableEvents, null);
    }
    
    public static void export(String outputPath, List<ExportableEvent> exportableEvents, ProcessingStats stats) throws IOException {
        System.out.println("[ParquetExporter] ParquetExporter.export called with outputPath: " + outputPath);
        System.out.println("[ParquetExporter] Number of exportable events: " + exportableEvents.size());

        // Collect statistics if provided
        if (stats != null) {
            for (ExportableEvent event : exportableEvents) {
                // Count by event type
                if (event.getEvent() != null) {
                    stats.incrementEventType(event.getEvent());
                }
                
                // Count by table operations
                String tableName = event.getTable();
                String eventType = event.getEvent();
                if (eventType != null && tableName != null) {
                    if (eventType.toLowerCase().contains("write") || eventType.toLowerCase().contains("insert")) {
                        stats.incrementTableOperation(tableName, "write");
                    } else if (eventType.toLowerCase().contains("update")) {
                        stats.incrementTableOperation(tableName, "update");
                    } else if (eventType.toLowerCase().contains("delete")) {
                        stats.incrementTableOperation(tableName, "delete");
                    }
                }
            }
        }

        // Group events by table name
        Map<String, List<ExportableEvent>> eventsByTable = groupEventsByTable(exportableEvents);
        System.out.println("[ParquetExporter] Found " + eventsByTable.size() + " tables");

        // Extract directory name from the output path (remove .parquet extension if present)
        String baseOutputPath = outputPath;
        if (baseOutputPath.endsWith(".parquet")) {
            baseOutputPath = baseOutputPath.substring(0, baseOutputPath.length() - 8);
        }

        // Create the output directory (clean if it already exists)
        java.io.File outputDir = new java.io.File(baseOutputPath);
        if (outputDir.exists()) {
            System.out.println("[ParquetExporter] Output directory exists, cleaning contents: " + baseOutputPath);
            cleanDirectory(outputDir);
        } else {
            System.out.println("[ParquetExporter] Creating output directory: " + baseOutputPath);
            if (!outputDir.mkdirs()) {
                throw new IOException("Failed to create output directory: " + baseOutputPath);
            }
        }

        // Export each table to its own parquet file
        for (Map.Entry<String, List<ExportableEvent>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<ExportableEvent> tableEvents = entry.getValue();
            
            String tableOutputPath = Paths.get(baseOutputPath, tableName + ".parquet").toString();
            System.out.println("[ParquetExporter] Exporting " + tableEvents.size() + " events for table '" + tableName + "' to: " + tableOutputPath);
            
            exportToFile(tableOutputPath, tableEvents);
        }

        System.out.println("[ParquetExporter] Export completed successfully to directory: " + baseOutputPath);
    }

    private static void exportToFile(String outputPath, List<ExportableEvent> exportableEvents) throws IOException {
        System.out.println("[ParquetExporter] exportToFile called with outputPath: " + outputPath);
        System.out.println("[ParquetExporter] Number of exportable events: " + exportableEvents.size());

        if (exportableEvents.isEmpty()) {
            System.out.println("[ParquetExporter] No exportable events to write to Parquet - creating empty file.");
            // Create empty Parquet file with proper schema
            MessageType schema = createExportableEventSchema();
            OutputFile outputFileImpl = new SimpleOutputFile(outputPath);

            // Check if parent directory exists
            java.io.File outputFile = new java.io.File(outputPath);
            java.io.File parentDir = outputFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                System.out.println("[ParquetExporter] Creating parent directory: " + parentDir.getAbsolutePath());
                if (!parentDir.mkdirs()) {
                    throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
                }
            }

            System.out.println("[ParquetExporter] Setting up schema for empty file");
            GroupWriteSupport.setSchema(schema, new Configuration());
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFileImpl)
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
                System.out.println("[ParquetExporter] ParquetWriter created for empty file, writing no records");
                // Write nothing - just create the empty file with schema
            }

            // Verify file was created
            if (outputFile.exists()) {
                System.out.println("[ParquetExporter] Created empty Parquet file: " + outputPath + " (size: " + outputFile.length() + " bytes)");
            } else {
                throw new IOException("Empty Parquet file was not created: " + outputPath);
            }
            return;
        }

        // Check if parent directory exists
        java.io.File outputFile = new java.io.File(outputPath);
        java.io.File parentDir = outputFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            System.out.println("[ParquetExporter] Creating parent directory: " + parentDir.getAbsolutePath());
            if (!parentDir.mkdirs()) {
                throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
            }
        }

        MessageType schema = createExportableEventSchema();
        OutputFile outputFileImpl = new SimpleOutputFile(outputPath);

        System.out.println("[ParquetExporter] Creating ParquetWriter for: " + outputPath);
        System.out.println("[ParquetExporter] Setting up schema...");
        GroupWriteSupport.setSchema(schema, new Configuration());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFileImpl)
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY) // Good compression for BigQuery
                .withRowGroupSize(16 * 1024 * 1024) // 16MB row groups for BigQuery optimization
                .withPageSize(512 * 1024) // 512KB pages for 15+ columns
                .withConf(getParquetConfig())
                .build()) {

            System.out.println("[ParquetExporter] ParquetWriter created successfully, writing " + exportableEvents.size() + " records");

            int recordCount = 0;
            for (ExportableEvent event : exportableEvents) {
                try {
                    Group record = createRecord(schema, event);
                    writer.write(record);
                    recordCount++;

                    // Check memory pressure every 1000 records
                    if (recordCount % 1000 == 0) {
                        Runtime runtime = Runtime.getRuntime();
                        long totalMemory = runtime.totalMemory();
                        long freeMemory = runtime.freeMemory();
                        long usedMemory = totalMemory - freeMemory;
                        long maxMemory = runtime.maxMemory();

                        double memoryUsagePercent = ((double) usedMemory / maxMemory) * 100;

                        if (memoryUsagePercent > 75.0) {
                            System.out.println("[ParquetExporter] High memory usage detected: " +
                                String.format("%.1f%% (%dMB/%dMB). Running GC...",
                                memoryUsagePercent, usedMemory/1024/1024, maxMemory/1024/1024));
                            System.gc();
                        }

                        if (recordCount % 5000 == 0) {
                            System.out.println("[ParquetExporter] Written " + recordCount + " records so far. Memory: " +
                                String.format("%.1f%% (%dMB/%dMB)",
                                memoryUsagePercent, usedMemory/1024/1024, maxMemory/1024/1024));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ParquetExporter] Error writing record " + recordCount + ": " + e.getMessage());
                    throw e;
                }
            }
            System.out.println("[ParquetExporter] Finished writing " + recordCount + " records, closing writer...");
        } catch (Exception e) {
            System.err.println("[ParquetExporter] Error during ParquetWriter operations: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Verify file was created and has content
        if (outputFile.exists()) {
            long fileSize = outputFile.length();
            System.out.println("[ParquetExporter] SUCCESS: Exported " + exportableEvents.size() + " events to Parquet file: " + outputPath + " (size: " + fileSize + " bytes)");

            if (fileSize == 0) {
                System.err.println("[ParquetExporter] ERROR: Created file has zero bytes!");
                throw new IOException("Created Parquet file is empty (0 bytes): " + outputPath);
            } else if (fileSize < 100 && !exportableEvents.isEmpty()) {
                System.err.println("[ParquetExporter] WARNING: Created file is suspiciously small for " + exportableEvents.size() + " events: " + fileSize + " bytes");
            }
        } else {
            throw new IOException("Parquet file was not created: " + outputPath);
        }
    }

    private static Long parsePositionAsLong(String position) {
        if (position == null || position.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(position);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseGtidTxnAsLong(String gtidTxn) {
        if (gtidTxn == null || gtidTxn.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(gtidTxn);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseTimestampStringAsLong(String timestampString) {
        if (timestampString == null || timestampString.isEmpty()) {
            return null;
        }
        try {
            // First try parsing as long (milliseconds)
            return Long.parseLong(timestampString);
        } catch (NumberFormatException e) {
            try {
                // Try parsing as datetime format: '2025-07-11 08:34:59'
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(timestampString,
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                return ldt.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
            } catch (Exception ex) {
                return null;
            }
        }
    }
}
