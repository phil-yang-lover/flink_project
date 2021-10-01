package com.yang.gmall.realtime.app.dws;

import com.yang.gmall.realtime.bean.ESProvinceStats;
import com.yang.gmall.realtime.bean.ProvinceStats;
import com.yang.gmall.realtime.utils.ClickHouseUtil;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*
        检查点 略
        * */
        //env.enableCheckpointing(5000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //TODO : 读取kafka的dwd_order_wide数据根据地区维度进行对订单数据聚合
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        //创建flinkSQL-动态表--设置水位线------如果里面有datastream api就需要execute
        tEnv.executeSql(
            "CREATE TABLE ORDER_WIDE (" +
                "province_id BIGINT," +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id STRING," +
                "split_total_amount DOUBLE," +
                "create_time STRING," +
                "order_time AS TO_TIMESTAMP(create_time)," +
                "WATERMARK FOR order_time AS order_time - INTERVAL '3' SECOND" +
                ") WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId) +")"
        );
        //TODO : 分组、开窗、聚合
        // 注意：字段没有顺序但是字段名应该和实体类（建表的字段名）相等，通过字段名匹配
        Table provinceStateTable = tEnv.sqlQuery(
             "SELECT " +
                "DATE_FORMAT(TUMBLE_START(order_time, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(order_time, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                "province_id, " +
                "province_name, " +
                "province_area_code area_code, " +
                "province_iso_code iso_code ," +
                "province_3166_2_code iso_3166_2 ," +
                "SUM(split_total_amount) order_amount, " +
                "COUNT(DISTINCT  order_id) order_count, " +
                //以秒为单位获取当前的Unix时间戳
                "UNIX_TIMESTAMP()*1000 ts " +
                "FROM ORDER_WIDE " +
                "GROUP BY TUMBLE(order_time, INTERVAL '10' SECOND)," +
                "province_id,province_name,province_area_code,province_iso_code,province_3166_2_code "
        );
        System.out.println("计算完毕");
        DataStream<ProvinceStats> provinceStatsDS =
            tEnv.toAppendStream(provinceStateTable, ProvinceStats.class);

        provinceStatsDS.print("ProvinceStats>>>>>>>>");
        //TODO : Sink到Clickhouse
        provinceStatsDS.addSink(
            ClickHouseUtil.getJdbcSink(
                "insert into  province_stats_2021  values(?,?,?,?,?,?,?,?,?,?)"
            )
        );
        //TODO : Sink到 Elasticsearch
        /*List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop2101", 9200, "http"));
        httpHosts.add(new HttpHost("hadoop2102", 9200, "http"));
        httpHosts.add(new HttpHost("hadoop2103", 9200, "http"));

        ElasticsearchSink.Builder<ProvinceStats> esSinkBuilder =
            new ElasticsearchSink.Builder<ProvinceStats>(
            httpHosts,
            new ElasticsearchSinkFunction<ProvinceStats>() {
                public IndexRequest createIndexRequest(ProvinceStats provinceStats) {
                    Map<String, ESProvinceStats> json = new HashMap<>();
                    ESProvinceStats esprovinceStats1 = new ESProvinceStats();
                    esprovinceStats1.setStt(provinceStats.getStt());
                    esprovinceStats1.setEdt(provinceStats.getEdt());
                    esprovinceStats1.setProvince_id(provinceStats.getProvince_id());
                    esprovinceStats1.setProvince_name(provinceStats.getProvince_name());
                    esprovinceStats1.setArea_code(provinceStats.getArea_code());
                    esprovinceStats1.setIso_code(provinceStats.getIso_code());
                    esprovinceStats1.setIso_3166_2(provinceStats.getIso_3166_2());
                    esprovinceStats1.setOrder_amount(Double.valueOf(provinceStats.getOrder_amount().doubleValue()));
                    esprovinceStats1.setOrder_count(provinceStats.getOrder_count());
                    esprovinceStats1.setTs(provinceStats.getTs());
                    json.put("data", esprovinceStats1);

                    return Requests.indexRequest()
                        .index("province")
                        .type("_doc")
                        .source(json);
                }
                @Override
                public void process(ProvinceStats provinceStats, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                    requestIndexer.add(createIndexRequest(provinceStats));
                }
            }
        );*/

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        //esSinkBuilder.setBulkFlushMaxActions(1);

// provide a RestClientFactory for custom configuration on the internally created REST client
        /*esSinkBuilder.setRestClientFactory(
            restClientBuilder -> {
                restClientBuilder.setDefaultHeaders(...)
                restClientBuilder.setMaxRetryTimeoutMillis(...)
                restClientBuilder.setPathPrefix(...)
                restClientBuilder.setHttpClientConfigCallback(...)
            }
        );*/
        //provinceStatsDS.addSink(esSinkBuilder.build());

        env.execute();
    }
}
