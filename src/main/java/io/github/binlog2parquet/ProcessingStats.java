package io.github.binlog2parquet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessingStats {
    private final Map<String, TableStats> tableStats = new HashMap<>();
    private final Map<String, AtomicLong> eventTypeCounts = new HashMap<>();
    
    public static class TableStats {
        public final AtomicLong writes = new AtomicLong(0);
        public final AtomicLong updates = new AtomicLong(0);
        public final AtomicLong deletes = new AtomicLong(0);
        
        public Map<String, Long> toMap() {
            Map<String, Long> map = new HashMap<>();
            long writesCount = writes.get();
            long updatesCount = updates.get();
            long deletesCount = deletes.get();
            long total = writesCount + updatesCount + deletesCount;
            
            map.put("writes", writesCount);
            map.put("updates", updatesCount);
            map.put("deletes", deletesCount);
            map.put("total", total);
            return map;
        }
    }
    
    public synchronized void incrementEventType(String eventType) {
        eventTypeCounts.computeIfAbsent(eventType, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    public synchronized void incrementTableOperation(String tableName, String operationType) {
        if (tableName == null || tableName.trim().isEmpty()) {
            tableName = "unknown_table";
        } else {
            tableName = sanitizeTableName(tableName);
        }
        
        TableStats stats = tableStats.computeIfAbsent(tableName, k -> new TableStats());
        
        switch (operationType.toLowerCase()) {
            case "write":
            case "insert":
                stats.writes.incrementAndGet();
                break;
            case "update":
                stats.updates.incrementAndGet();
                break;
            case "delete":
                stats.deletes.incrementAndGet();
                break;
        }
    }
    
    private String sanitizeTableName(String tableName) {
        return tableName.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
    
    public Map<String, Map<String, Long>> getTableStats() {
        Map<String, Map<String, Long>> result = new HashMap<>();
        for (Map.Entry<String, TableStats> entry : tableStats.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toMap());
        }
        return result;
    }
    
    public Map<String, Long> getEventTypeCounts() {
        Map<String, Long> result = new HashMap<>();
        for (Map.Entry<String, AtomicLong> entry : eventTypeCounts.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
    
    public long getTotalEvents() {
        return eventTypeCounts.values().stream().mapToLong(AtomicLong::get).sum();
    }
    
    // Metadata fields
    private Long serverId;
    private String binaryLog;
    private String priorGtids;
    private String mergedGtids;
    private String nextLog;
    private Long earliestTimestamp;
    private Long latestTimestamp;
    private String earliestUtc;
    private String latestUtc;
    private Long fileSizeBytes;
    
    // Performance timing fields
    private Long totalTime;
    private Long s3DownloadTime;
    private Long fileScanningTime;
    private Long parquetGenerationTime;
    private Long uploadTime;
    
    public void setMetadata(long serverId, String binaryLog, String priorGtids, String mergedGtids, 
                           String nextLog, Long earliestTimestamp, Long latestTimestamp,
                           String earliestUtc, String latestUtc) {
        this.serverId = serverId;
        this.binaryLog = binaryLog;
        this.priorGtids = priorGtids;
        this.mergedGtids = mergedGtids;
        this.nextLog = nextLog;
        this.earliestTimestamp = earliestTimestamp;
        this.latestTimestamp = latestTimestamp;
        this.earliestUtc = earliestUtc;
        this.latestUtc = latestUtc;
    }
    
    public void setFileSizeBytes(long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }
    
    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }
    
    public void setS3DownloadTime(long s3DownloadTime) {
        this.s3DownloadTime = s3DownloadTime;
    }
    
    public void setFileScanningTime(long fileScanningTime) {
        this.fileScanningTime = fileScanningTime;
    }
    
    public void setParquetGenerationTime(long parquetGenerationTime) {
        this.parquetGenerationTime = parquetGenerationTime;
    }
    
    public void setUploadTime(long uploadTime) {
        this.uploadTime = uploadTime;
    }
    
    public Map<String, Object> getMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        if (serverId != null) metadata.put("serverId", serverId);
        if (binaryLog != null) metadata.put("binaryLog", binaryLog);
        if (priorGtids != null) metadata.put("priorGtids", priorGtids);
        if (mergedGtids != null) metadata.put("mergedGtids", mergedGtids);
        if (nextLog != null) metadata.put("nextLog", nextLog);
        if (earliestTimestamp != null) metadata.put("earliestTimestamp", earliestTimestamp);
        if (latestTimestamp != null) metadata.put("latestTimestamp", latestTimestamp);
        if (earliestUtc != null) metadata.put("earliestUtc", earliestUtc);
        if (latestUtc != null) metadata.put("latestUtc", latestUtc);
        if (fileSizeBytes != null) metadata.put("fileSizeBytes", fileSizeBytes);
        return metadata;
    }
    
    public Map<String, Long> getPerformance() {
        Map<String, Long> performance = new HashMap<>();
        if (totalTime != null) performance.put("totalTime", totalTime);
        if (s3DownloadTime != null) performance.put("s3DownloadTime", s3DownloadTime);
        if (fileScanningTime != null) performance.put("fileScanningTime", fileScanningTime);
        if (parquetGenerationTime != null) performance.put("parquetGenerationTime", parquetGenerationTime);
        if (uploadTime != null) performance.put("uploadTime", uploadTime);
        return performance;
    }
    
    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("tableStats", getTableStats());
        result.put("eventTypeCounts", getEventTypeCounts());
        result.put("totalEvents", getTotalEvents());
        result.put("metadata", getMetadata());
        result.put("perf", getPerformance());
        return result;
    }
}