package io.github.binlog2parquet;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MemoryMonitor {
    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final AtomicLong lastLogTime = new AtomicLong(System.currentTimeMillis());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static volatile boolean isMonitoring = false;
    private static volatile java.io.PrintStream originalErr = System.err;

    public static void startPeriodicMonitoring(int intervalSeconds) {
        if (isMonitoring) {
            return;
        }
        
        isMonitoring = true;
        originalErr = System.err; // Capture the original stderr
        logToStderr("[MemoryMonitor] Starting periodic memory monitoring every " + intervalSeconds + " seconds");
        
        scheduler.scheduleAtFixedRate(() -> {
            logMemoryUsage("PERIODIC");
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public static void stopPeriodicMonitoring() {
        isMonitoring = false;
        scheduler.shutdown();
        logToStderr("[MemoryMonitor] Stopped periodic memory monitoring");
    }
    
    private static void logToStderr(String message) {
        String timestamp = LocalDateTime.now().format(formatter);
        originalErr.println(timestamp + " " + message);
    }

    public static void logMemoryUsage(String context) {
        try {
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
            
            long usedMB = heapUsage.getUsed() / (1024 * 1024);
            long committedMB = heapUsage.getCommitted() / (1024 * 1024);
            long maxMB = heapUsage.getMax() / (1024 * 1024);
            
            double usagePercentage = maxMB > 0 ? (double) usedMB / maxMB * 100 : 0;
            
            long nonHeapUsedMB = nonHeapUsage.getUsed() / (1024 * 1024);
            long nonHeapCommittedMB = nonHeapUsage.getCommitted() / (1024 * 1024);
            
            // Get free memory
            long freeMB = maxMB > 0 ? maxMB - usedMB : (committedMB - usedMB);
            
            String logMessage = String.format(
                "[MemoryMonitor] %s - Heap: %dMB used / %dMB committed / %dMB max (%.1f%% used, %dMB free) | Non-Heap: %dMB used / %dMB committed", 
                context, usedMB, committedMB, maxMB, usagePercentage, freeMB, nonHeapUsedMB, nonHeapCommittedMB
            );
            
            logToStderr(logMessage);
            
            // Warn if memory usage is high
            if (usagePercentage > 80) {
                logToStderr("[MemoryMonitor] WARNING: High memory usage detected (" + String.format("%.1f", usagePercentage) + "%)");
                if (usagePercentage > 95) {
                    logToStderr("[MemoryMonitor] CRITICAL: Memory usage is critically high! Consider running garbage collection or increasing heap size.");
                    // Force garbage collection in critical situations
                    System.gc();
                    logMemoryUsage("POST_GC");
                }
            }
            
        } catch (Exception e) {
            logToStderr("[MemoryMonitor] Error getting memory usage: " + e.getMessage());
        }
    }

    public static void logMemoryUsageWithThrottle(String context, int minIntervalSeconds) {
        long currentTime = System.currentTimeMillis();
        long lastLog = lastLogTime.get();
        
        if (currentTime - lastLog >= minIntervalSeconds * 1000) {
            if (lastLogTime.compareAndSet(lastLog, currentTime)) {
                logMemoryUsage(context);
            }
        }
    }

    public static MemoryUsage getHeapMemoryUsage() {
        return memoryBean.getHeapMemoryUsage();
    }

    public static String getMemorySummary() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        long usedMB = heapUsage.getUsed() / (1024 * 1024);
        long maxMB = heapUsage.getMax() / (1024 * 1024);
        double usagePercentage = maxMB > 0 ? (double) usedMB / maxMB * 100 : 0;
        
        return String.format("%dMB used / %dMB max (%.1f%%)", usedMB, maxMB, usagePercentage);
    }

    public static boolean isMemoryLow(double thresholdPercentage) {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        long usedMB = heapUsage.getUsed() / (1024 * 1024);
        long maxMB = heapUsage.getMax() / (1024 * 1024);
        double usagePercentage = maxMB > 0 ? (double) usedMB / maxMB * 100 : 0;
        
        return usagePercentage > thresholdPercentage;
    }
}