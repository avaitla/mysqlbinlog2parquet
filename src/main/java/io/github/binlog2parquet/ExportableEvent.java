package io.github.binlog2parquet;

public class ExportableEvent {
    private long counter;
    private String binarylog;
    private String position;
    private String gtidPosition;
    private String gtid;
    private String gtidUuid;
    private String gtidTxn;
    private long serverId;
    private long timestamp;
    private String timestampString;
    private String event;
    private String table;
    private String query;
    private String queryFingerprint;
    private String queryHash;
    private String data;
    private String old;
    private String changed;
    private String columns;
    private String pk;

    public ExportableEvent(long counter, String binarylog, String position, String gtidPosition, String gtid,
                          long serverId, long timestamp, String timestampString, String event, String table,
                          String query, String queryFingerprint, String queryHash,
                          String data, String old, String changed, String columns) {

        this.counter = counter;
        this.binarylog = binarylog;
        this.position = position;
        this.gtidPosition = gtidPosition;
        this.gtid = gtid;
        this.gtidUuid = extractGtidUuid(gtid);
        this.gtidTxn = extractGtidTxn(gtid);
        this.serverId = serverId;
        this.timestamp = timestamp;
        this.timestampString = timestampString;
        this.event = event;
        this.table = table;
        this.query = query;
        this.queryFingerprint = queryFingerprint;
        this.queryHash = queryHash;
        this.data = data;
        this.old = old;
        this.changed = changed;
        this.columns = columns;
        this.pk = this.gtidUuid + ":" + this.gtidTxn + ":" + serverId + ":" + binarylog + ":" + position + ":" + counter;
    }

    // Getters
    public long getCounter() { return counter; }
    public String getBinarylog() { return binarylog; }
    public String getPosition() { return position; }
    public String getGtidPosition() { return gtidPosition; }
    public String getGtid() { return gtid; }
    public String getGtidUuid() { return gtidUuid; }
    public String getGtidTxn() { return gtidTxn; }
    public long getServerId() { return serverId; }
    public long getTimestamp() { return timestamp; }
    public String getTimestampString() { return timestampString; }
    public String getEvent() { return event; }
    public String getTable() { return table; }
    public String getQuery() { return query; }
    public String getQueryFingerprint() { return queryFingerprint; }
    public String getQueryHash() { return queryHash; }
    public String getData() { return data; }
    public String getOld() { return old; }
    public String getChanged() { return changed; }
    public String getColumns() { return columns; }
    public String getPk() { return pk; }

    @Override
    public String toString() {
        return "ExportableEvent{" +
               "counter=" + counter +
               ", binarylog='" + binarylog + '\'' +
               ", position='" + position + '\'' +
               ", gtidPosition='" + gtidPosition + '\'' +
               ", gtid='" + gtid + '\'' +
               ", gtidUuid='" + gtidUuid + '\'' +
               ", gtidTxn='" + gtidTxn + '\'' +
               ", serverId=" + serverId +
               ", timestamp=" + timestamp +
               ", timestampString='" + timestampString + '\'' +
               ", event='" + event + '\'' +
               ", table='" + table + '\'' +
               ", query='" + query + '\'' +
               ", queryFingerprint='" + queryFingerprint + '\'' +
               ", queryHash='" + queryHash + '\'' +
               ", data='" + data + '\'' +
               ", old='" + old + '\'' +
               ", changed='" + changed + '\'' +
               ", columns='" + columns + '\'' +
               '}';
    }

    private String extractGtidUuid(String gtid) {
        if (gtid == null || gtid.isEmpty()) {
            return null;
        }
        int colonIndex = gtid.indexOf(':');
        return colonIndex > 0 ? gtid.substring(0, colonIndex) : null;
    }

    private String extractGtidTxn(String gtid) {
        if (gtid == null || gtid.isEmpty()) {
            return null;
        }
        int colonIndex = gtid.indexOf(':');
        return colonIndex >= 0 && colonIndex < gtid.length() - 1 ? gtid.substring(colonIndex + 1) : null;
    }
}
