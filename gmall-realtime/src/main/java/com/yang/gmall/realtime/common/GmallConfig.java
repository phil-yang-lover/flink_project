package com.yang.gmall.realtime.common;

public class GmallConfig {
    public static final String HBASE_SCHEMA="GMALL1021_REALTIME";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop2101,hadoop2102,hadoop2103:2181";
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop2102:8123/default";
}
