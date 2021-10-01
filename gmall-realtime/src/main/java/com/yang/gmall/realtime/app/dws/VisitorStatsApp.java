package com.yang.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.bean.VisitorStats;
import com.yang.gmall.realtime.utils.ClickHouseUtil;
import com.yang.gmall.realtime.utils.MyDateUtil;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: Felix
 * Date: 2021/4/26
 * Desc: 访客主题统计应用
 * 维度：版本、渠道、地区、新老访客
 * 度量：pv、uv、ujc、druing_time、sv_c
 * 测试流程
 * -需要启动的进程
 * zk、kafka、logger.sh、hdfs
 * -需要启动的应用程序
 * BaseLogApp---分流
 * UniqueVisitApp---独立访客
 * UserJumpDetailApp----用户跳出明细
 * VisitorStatsApp ----访客主题统计
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 并行度
        env.setParallelism(4);

        //1.2 检查点
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop2101:9820/gmall/checkpointing"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO 2.从kafka主题中读取数据
        String groupId = "visitor_stats_group";
        //2.1  读取页面访问日志 dwd_page_log   pv  during sv

        DataStreamSource<String> dwdPageLogSource =
            env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", groupId));

        //2.2 读取独立访客情况  dwm_unique_visit    uv
        DataStreamSource<String> dwmUniqueVisitSource =
            env.addSource(MyKafkaUtil.getKafkaSource("dwm_unique_visit", groupId));

        //2.3 读取用户跳出明细  dwm_user_jump_detail  ujd
        DataStreamSource<String> dwmUserJumpDetailSource =
            env.addSource(MyKafkaUtil.getKafkaSource("dwm_user_jump_detail", groupId));

        //TODO 3.对读取的数据进行结构的转换   VisitorStats
        //3.1 页面日志流
        SingleOutputStreamOperator<VisitorStats> pageLogStatsDS = dwdPageLogSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                VisitorStats visitorStats = new VisitorStats(
                    "",
                    "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L,
                    1L,
                    0L,
                    0L,
                    jsonObj.getJSONObject("page").getLong("during_time"),
                    jsonObj.getLong("ts")
                );
                String lastPageId =
                    jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    visitorStats.setSv_ct(1L);
                }
                return visitorStats;
            }
        });

        //3.2 独立访客流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDS = dwmUniqueVisitSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                VisitorStats visitorStats = new VisitorStats(
                    "",
                    "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        //3.3 用户跳出流
        SingleOutputStreamOperator<VisitorStats> userJumpDetailStatsDS = dwmUserJumpDetailSource.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                VisitorStats visitorStats = new VisitorStats(
                    "",
                    "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });

        //TODO 4.将不同流的数据合并在一起形成一条流  union
        DataStream<VisitorStats> unionDS =
            pageLogStatsDS.union(uniqueVisitStatsDS, userJumpDetailStatsDS);

        //TODO 5.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        //TODO 6.按照维度进行分组  Tuple4<版本、渠道、地区、新老访客>
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsKeyedDS = visitorStatsWatermarkDS.keyBy(
            new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                    return Tuple4.of(
                        value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new()
                    );
                }
            });

        //TODO 7.对分组之后的数据进行开窗     window()
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS =
            visitorStatsKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 8.对开窗后的数据进行统计  reduce
        SingleOutputStreamOperator<VisitorStats> reduceVisitorStatsDS = windowDS.reduce(
            new ReduceFunction<VisitorStats>() {
                @Override
                public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                    value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                    value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                    value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                    value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                    value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                    return value1;
                }
            },
            new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                @Override
                public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                    //补全统计的其实和结束时间
                    for (VisitorStats visitorStats : iterable) {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        visitorStats.setStt(MyDateUtil.toYMDhms(new Date(start)));
                        visitorStats.setEdt(MyDateUtil.toYMDhms(new Date(end)));
                        //向下游传递窗口中的元素
                        collector.collect(visitorStats);
                    }
                }
            }
        );
        reduceVisitorStatsDS.print();

        //TODO 9.将聚合结构写到Clickhouse
        //     jdbcSink只能处理流写入单个表。因为其他程序处理的流也要写入clickhouse中，所以抽取类
        String sql = "insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)";
        reduceVisitorStatsDS.addSink(ClickHouseUtil.getJdbcSink(sql));

        env.execute();
    }
}
