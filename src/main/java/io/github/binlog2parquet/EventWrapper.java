package io.github.binlog2parquet;

import com.github.shyiko.mysql.binlog.event.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.Serializable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class EventWrapper {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return obj.toString(); // fallback
        }
    }

    /**
     * Result object for statement digest functions
     */
    public static class StatementDigestResult {
        public final String query;
        public final String digest;
        public final String digestText;

        public StatementDigestResult(String query, String digest, String digestText) {
            this.query = query;
            this.digest = digest;
            this.digestText = digestText;
        }

        @Override
        public String toString() {
            return "StatementDigestResult{" +
                "query='" + query + '\'' +
                ", digest='" + digest + '\'' +
                ", digestText='" + digestText + '\'' +
                '}';
        }
    }


    public final Event event;
    public final long position;
    public final String timestampUtc;
    public final String fname;
    public final long serverId;

    public Integer txCount = null;
    public EventWrapper lastGtidEventWrapper;
    public Map<Long, EventWrapper> tableMapEventWrapper;
    public EventWrapper lastRowsQueryEventWrapper;

    public StatementDigestResult statementDigestResult;
    public List<EventWrapper> gtidEventsWrapper;

    private static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public EventWrapper(Event event, long position, String fname, EventWrapper lastGtidEventWrapper, long serverId) {
        this.event = event;
        this.position = position;
        this.timestampUtc = convertTimestampToUtcString(event.getHeader().getTimestamp());
        this.fname = fname;
        this.lastGtidEventWrapper = lastGtidEventWrapper;
        this.serverId = serverId;
    }


    public EventWrapper(Event event, long position, String fname, EventWrapper lastGtidEventWrapper,
                        Map<Long, EventWrapper> tableMapEventWrapper, EventWrapper lastRowsQueryEventWrapper,
                        long serverId) {
        this.event = event;
        this.position = position;
        this.timestampUtc = convertTimestampToUtcString(event.getHeader().getTimestamp());
        this.fname = fname;
        this.lastGtidEventWrapper = lastGtidEventWrapper;
        this.tableMapEventWrapper = tableMapEventWrapper;
        this.lastRowsQueryEventWrapper = lastRowsQueryEventWrapper;
        this.serverId = serverId;
    }

    private String convertTimestampToUtcString(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return utcDateTime.format(UTC_FORMATTER);
    }

    public void setLastGtidEventWrapper(EventWrapper gtidEventWrapper) {
        this.lastGtidEventWrapper = gtidEventWrapper;
    }

    public void setDigest(StatementDigestResult digestResult) {
        this.statementDigestResult = digestResult;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("At{").append(position).append("} ");
        sb.append("[").append(timestampUtc).append(" UTC] ");

        if (lastGtidEventWrapper != null) {
            GtidEventData gd = (GtidEventData) lastGtidEventWrapper.event.getData();
            sb.append("GTID Part =[").append(gd.getMySqlGtid()).append("] ");
        } else if (event.getData() instanceof GtidEventData) {
            if (gtidEventsWrapper != null) {
                GtidEventData gd = (GtidEventData) event.getData();
                sb.append("GTID Start=[").append(gd.getMySqlGtid()).append("] - ").append(gtidEventsWrapper.size()).append(" events ");
            }
        }

        sb.append(event);

        return sb.toString();
    }

    public List<ExportableEvent> export(long startingCounter) {
        EventType eventType = event.getHeader().getEventType();

        if (eventType == EventType.EXT_UPDATE_ROWS ||
            eventType == EventType.EXT_WRITE_ROWS ||
            eventType == EventType.EXT_DELETE_ROWS) {

            //System.out.println("Found exportable row event: " + eventType);

            // Extract data from referenced events
            String gtid = lastGtidEventWrapper != null ?
                ((GtidEventData) lastGtidEventWrapper.event.getData()).getMySqlGtid().toString() : null;
            String gtidPosition = lastGtidEventWrapper != null ?
                String.valueOf(lastGtidEventWrapper.position) : null;

            //if (gtidPosition.equals("50415995")) {
            //    System.out.println("Found target event");
            //}
            String query = lastRowsQueryEventWrapper != null ? ((RowsQueryEventData) lastRowsQueryEventWrapper.event.getData()).getQuery() : null;

            String queryFingerprint = null;
            String queryHash = null;

            if (lastRowsQueryEventWrapper != null && lastRowsQueryEventWrapper.statementDigestResult != null) {
                queryFingerprint = lastRowsQueryEventWrapper.statementDigestResult.digestText;
                queryHash = lastRowsQueryEventWrapper.statementDigestResult.digest;
            }

            // Extract row data based on event type
            String table = "";
            String oldData = null;
            String changedData = null;
            String newData = event.getData().toString(); // Default data representation
            String columns = null;

            if (eventType == EventType.EXT_UPDATE_ROWS || eventType == EventType.UPDATE_ROWS) {
                UpdateRowsEventData updateData = (UpdateRowsEventData) event.getData();
                long tableId = updateData.getTableId();
                EventWrapper lastTableMapEventWrapper = tableMapEventWrapper.get(tableId);

                TableMapEventData tableMapData = (TableMapEventData) lastTableMapEventWrapper.event.getData();
                table = tableMapData.getDatabase() + "." + tableMapData.getTable();
                List<String> cols = tableMapData.getEventMetadata().getColumnNames();
                String[] columnNames = cols.toArray(new String[0]);

                if (!updateData.getRows().isEmpty() && columnNames != null) {
                    List<Map.Entry<Serializable[], Serializable[]>> rows = updateData.getRows();
                    List<ExportableEvent> events = new ArrayList<>();
                    long currentCounter = startingCounter;

                    if (rows.size() > 1) {
                        //System.out.println("Bulk Entry Detected");
                    }

                    for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                        Serializable[] beforeRow = row.getKey();
                        Serializable[] afterRow = row.getValue();
                        assert beforeRow.length == afterRow.length;

                        // Build old data map
                        Map<String, Object> oldMap = new LinkedHashMap<>();
                        for (int i = 0; i < beforeRow.length; i++) {
                            oldMap.put(columnNames[i], beforeRow[i]);
                        }
                        String rowOldData = toJson(oldMap);

                        // Build new data map
                        Map<String, Object> newMap = new LinkedHashMap<>();
                        for (int i = 0; i < afterRow.length; i++) {
                            newMap.put(columnNames[i], afterRow[i]);
                        }
                        String rowNewData = toJson(newMap);

                        // Build changed data map and columns list
                        Map<String, Map<String, Object>> changedMap = new LinkedHashMap<>();
                        List<String> changedColumnsList = new ArrayList<>();
                        for (int i = 0; i < afterRow.length; i++) {
                            Object oldValue = beforeRow[i];
                            Object newValue = afterRow[i];

                            boolean areEqual;
                            if (oldValue instanceof byte[] && newValue instanceof byte[]) {
                                areEqual = Arrays.equals((byte[]) oldValue, (byte[]) newValue);
                            } else if (oldValue instanceof Object[] && newValue instanceof Object[]) {
                                areEqual = Arrays.deepEquals((Object[]) oldValue, (Object[]) newValue);
                            } else {
                                areEqual = Objects.equals(oldValue, newValue);
                            }

                            if (!areEqual) {
                                Map<String, Object> changeDetail = new LinkedHashMap<>();
                                changeDetail.put("old", oldValue);
                                changeDetail.put("new", newValue);
                                changedMap.put(columnNames[i], changeDetail);
                                changedColumnsList.add(columnNames[i]);
                            }
                        }

                        String rowChangedData = toJson(changedMap);
                        String rowColumns = toJson(changedColumnsList);

                        events.add(new ExportableEvent(
                            currentCounter++, // counter
                            fname, // binarylog
                            String.valueOf(position), // position
                            gtidPosition, // gtidPosition
                            gtid, // gtid
                            serverId, // serverId
                            event.getHeader().getTimestamp(), // timestamp
                            timestampUtc, // timestampString
                            eventType.name(), // event
                            table, // table
                            query, // query
                            queryFingerprint, // queryFingerprint
                            queryHash, // queryHash
                            rowNewData, // data
                            rowOldData, // old
                            rowChangedData, // changed
                            rowColumns  // columns
                        ));
                    }
                    return events;
                }
            } else if (eventType == EventType.EXT_DELETE_ROWS) {
                DeleteRowsEventData deleteData = (DeleteRowsEventData) event.getData();
                long tableId = deleteData.getTableId();
                EventWrapper lastTableMapEventWrapper = tableMapEventWrapper.get(tableId);

                TableMapEventData tableMapData = (TableMapEventData) lastTableMapEventWrapper.event.getData();
                table = tableMapData.getDatabase() + "." + tableMapData.getTable();
                List<String> cols = tableMapData.getEventMetadata().getColumnNames();
                String[] columnNames = cols.toArray(new String[0]);

                if (!deleteData.getRows().isEmpty() && columnNames != null) {
                    List<Serializable[]> rows = deleteData.getRows();
                    List<ExportableEvent> events = new ArrayList<>();
                    long currentCounter = startingCounter;

                    for (Serializable[] row : rows) {
                        // Build old data map (deleted row data)
                        Map<String, Object> oldMap = new LinkedHashMap<>();
                        for (int i = 0; i < row.length; i++) {
                            oldMap.put(columnNames[i], row[i]);
                        }
                        String rowOldData = toJson(oldMap);
                        String rowNewData = toJson(oldMap); // For delete, data is the deleted row

                        // For DELETE, all columns are affected
                        List<String> allColumnsList = Arrays.asList(columnNames);
                        String rowColumns = toJson(allColumnsList);

                        events.add(new ExportableEvent(
                            currentCounter++, // counter
                            fname, // binarylog
                            String.valueOf(position), // position
                            gtidPosition, // gtidPosition
                            gtid, // gtid
                            serverId, // serverId
                            event.getHeader().getTimestamp(), // timestamp
                            timestampUtc, // timestampString
                            eventType.name(), // event
                            table, // table
                            query, // query
                            queryFingerprint, // queryFingerprint
                            queryHash, // queryHash
                            rowNewData, // data
                            rowOldData, // old
                            null, // changed (null for deletes)
                            rowColumns  // columns
                        ));
                    }
                    return events;
                }
            } else if (eventType == EventType.EXT_WRITE_ROWS) {
                WriteRowsEventData writeData = (WriteRowsEventData) event.getData();
                long tableId = writeData.getTableId();
                EventWrapper lastTableMapEventWrapper = tableMapEventWrapper.get(tableId);

                TableMapEventData tableMapData = (TableMapEventData) lastTableMapEventWrapper.event.getData();
                table = tableMapData.getDatabase() + "." + tableMapData.getTable();
                List<String> cols = tableMapData.getEventMetadata().getColumnNames();
                String[] columnNames = cols.toArray(new String[0]);

                if (!writeData.getRows().isEmpty() && columnNames != null) {
                    List<Serializable[]> rows = writeData.getRows();
                    List<ExportableEvent> events = new ArrayList<>();
                    long currentCounter = startingCounter;

                    for (Serializable[] row : rows) {
                        // Build new data map (inserted row data)
                        Map<String, Object> newMap = new LinkedHashMap<>();
                        for (int i = 0; i < row.length; i++) {
                            newMap.put(columnNames[i], row[i]);
                        }
                        String rowNewData = toJson(newMap);

                        // For INSERT, all columns are affected
                        List<String> allColumnsList = Arrays.asList(columnNames);
                        String rowColumns = toJson(allColumnsList);

                        events.add(new ExportableEvent(
                            currentCounter++, // counter
                            fname, // binarylog
                            String.valueOf(position), // position
                            gtidPosition, // gtidPosition
                            gtid, // gtid
                            serverId, // serverId
                            event.getHeader().getTimestamp(), // timestamp
                            timestampUtc, // timestampString
                            eventType.name(), // event
                            table, // table
                            query, // query
                            queryFingerprint, // queryFingerprint
                            queryHash, // queryHash
                            rowNewData, // data
                            null, // old (null for inserts)
                            null, // changed (null for inserts)
                            rowColumns  // columns
                        ));
                    }
                    return events;
                }
            }

            // Return empty list if no rows to process
            return new ArrayList<>();
        }

        return Collections.emptyList();
    }
}
