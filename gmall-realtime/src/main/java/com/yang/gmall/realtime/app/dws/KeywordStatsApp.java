package com.yang.gmall.realtime.app.dws;

import com.yang.gmall.realtime.app.func.KeywordUDTF;
import com.yang.gmall.realtime.bean.KeywordStats;
import com.yang.gmall.realtime.common.GmallConstant;
import com.yang.gmall.realtime.utils.ClickHouseUtil;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TODO 搜索关键字计算*/
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO : 自己照着写的有问题
        /*StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        *//**
         * TODO : 检查点 略
         * *//*
        *//*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop2101:9820/gmall/checkpointing"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*//*

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //TODO 2.注册自定义函数
        tEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String pageLogSourceTopic = "dwd_page_log";

        tEnv.executeSql(" create table page_view( " +
            " common MAP<STRING,STRING>, " +
            " page MAP<STRING,STRING>, " +
            " ts BIGINT, " +
            " rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH：mm：ss')), " +   //将时间戳long->TIMESTAMP
            " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND " +
            ") WITH(" + MyKafkaUtil.getKafkaDDL(pageLogSourceTopic,groupId) + ")");
        //TODO 4.对数据进行过滤，只保留搜索日志
        Table fullwordView = tEnv.sqlQuery("" +
            " select page['item'] fullword,rowtime " +
            " from page_view " +
            " where page['page_id'] = 'good_list' " +
            " and page['item'] IS NOT NULL ");
        //TODO 5.使用自定义的分词函数，对搜索关键词进行分词
        Table keywordView = tEnv.sqlQuery("" +
            " select rowtime,keyword " +
            " from " + fullwordView + "," +
            " LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)" );
        System.out.println("分词完成");
        //TODO 6.根据各个关键词出现次数进行ct
        Table keywordStatsSearch = tEnv.sqlQuery("select " +
            "keyword," +
            "count(*) ct,'"
            + GmallConstant.KEYWORD_SEARCH + "' source ," +
            "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
            "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
            "UNIX_TIMESTAMP()*1000 ts " +
            " from " + keywordView + "" +
            " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword");
        System.out.println("分词统计");
        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsDS =
            tEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);
        keywordStatsDS.print("keywordStats>>>>>>>>>>>>>>>>>>");*/
        //TODO 8.写入到ClickHouse
        /*keywordStatsDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into keyword_stats_1021(keyword,ct,source,stt,edt,ts)  " +
                    " values(?,?,?,?,?,?)"
            )
        );*/

        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3. 从kafka中读取数据，创建动态表
        String groupId = "keyword_stats_app";
        String pageLogSourceTopic = "dwd_page_log";

        tableEnv.executeSql("create table page_view (" +
            "common MAP<STRING,STRING>," +
            "page MAP<STRING,STRING>," +
            "ts BIGINT," +
            "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
            "WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND" +
            ") WITH("+ MyKafkaUtil.getKafkaDDL(pageLogSourceTopic,groupId) +")");

        //TODO 4.对数据进行过滤，只保留搜索日志
        Table fullwordView  = tableEnv.sqlQuery("select page['item'] fullword,rowtime from page_view " +
            " where page['page_id'] = 'good_list' " +
            " and page['item'] IS NOT NULL");

        //TODO 5.使用自定义的分词函数，对搜索关键词进行分词
        Table keywordView = tableEnv.sqlQuery("select rowtime,keyword " +
            " from " + fullwordView +", LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)" );


        //TODO 6.分组、开窗、聚合
        Table keywordStatsSearch   = tableEnv.sqlQuery("select " +
            "keyword," +
            "count(*) ct,'"
            + GmallConstant.KEYWORD_SEARCH + "' source ," +
            "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
            "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
            "UNIX_TIMESTAMP()*1000 ts " +
            " from " + keywordView + "" +
            " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword");
        System.out.println("分词统计");
        //TODO 7.将表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsDS.print("keywordStats>>>");

        //TODO 8.将流中数据写到CK中
        keywordStatsDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts)  " +
                " values(?,?,?,?,?,?)"
            )
        );
        env.execute();
    }
}
