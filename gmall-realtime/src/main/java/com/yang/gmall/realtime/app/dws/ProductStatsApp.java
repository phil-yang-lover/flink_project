package com.yang.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.app.func.DimAsyncFunction;
import com.yang.gmall.realtime.bean.OrderWide;
import com.yang.gmall.realtime.bean.PaymentWide;
import com.yang.gmall.realtime.bean.ProductStats;
import com.yang.gmall.realtime.common.GmallConstant;
import com.yang.gmall.realtime.utils.ClickHouseUtil;
import com.yang.gmall.realtime.utils.MyDateUtil;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TumbleWithSize;
import org.apache.flink.table.runtime.operators.window.assigners.CountTumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO : 环境配置、并行度、检查点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop2101:9092/gmall/checkpointing"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/
        //TODO : 从kafka读取数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfosourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic  = "dwd_comment_info";

        DataStreamSource<String> pageViewDS = env.addSource(
            MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId)
        );
        DataStreamSource<String> orderWideDS = env.addSource(
            MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId)
        );
        DataStreamSource<String> paymentWideDS = env.addSource(
            MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId)
        );
        DataStreamSource<String> cartInfoDS = env.addSource(
            MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId)
        );
        DataStreamSource<String> favorInfoDS = env.addSource(
            MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId)
        );
        DataStreamSource<String> refundInfoDS = env.addSource(
            MyKafkaUtil.getKafkaSource(refundInfosourceTopic, groupId)
        );
        DataStreamSource<String> commentInfoDS = env.addSource(
            MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId)
        );
        //TODO : 转换数据结构
        SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDS = pageViewDS.flatMap(
            new FlatMapFunction<String, ProductStats>() {

                @Override
                public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(value);
                    //

                    JSONObject page = jsonObj.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    Long ts = jsonObj.getLong("ts");
                    if (pageId.equals("good_detail")) {
                        Long skuId = page.getLong("item");
                        ProductStats productStats = ProductStats.builder()
                            .sku_id(skuId)
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                        out.collect(productStats);
                    }
                    // TODO : 因为一个页面有多个曝光商品，map只能一对一返回值，所以不能使用map
                    //         ***老师在这里没用flatmap，说是这个就是集合扁平化，而是用的process
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            if (displaysJSONObject.getString("item_type").equals("sku_id")) {
                                Long skuId = displaysJSONObject.getLong("item");
                                ProductStats productStats = ProductStats.builder()
                                    .ts(ts)
                                    .sku_id(skuId)
                                    .display_ct(1L)
                                    .build();
                                out.collect(productStats);
                            }
                        }
                    }
                }
            }
        );
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String value) throws Exception {
                    OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                    return ProductStats.builder()
                        .order_amount(orderWide.getSplit_total_amount())
                        .order_sku_num(orderWide.getSku_num())
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .sku_id(orderWide.getSku_id())
                        .ts(MyDateUtil.toTs(orderWide.getCreate_time()))
                        .build();
                }
            }
        );
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String value) throws Exception {
                    PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);
                    return ProductStats.builder()
                        .ts(MyDateUtil.toTs(paymentWide.getOrder_create_time()))
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .build();
                }
            }
        );
        SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String value) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(value);
                    return ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .ts(MyDateUtil.toTs(jsonObj.getString("create_time")))
                        .cart_ct(1L)
                        .build();
                }
            }
        );
        SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String value) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(value);
                    return ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .ts(MyDateUtil.toTs(jsonObj.getString("create_time")))
                        .favor_ct(1L)
                        .build();
                }
            }
        );
        SingleOutputStreamOperator<ProductStats> refundInfoStatsDS = refundInfoDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String value) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(value);
                    return ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .ts(MyDateUtil.toTs(jsonObj.getString("create_time")))
                        .refund_amount(jsonObj.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(new HashSet(Collections.singleton(jsonObj.getLong("order_id"))))
                        .build();
                }
            }
        );
        SingleOutputStreamOperator<ProductStats> commentInfoStatsDS = commentInfoDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String value) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(value);
                    return ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .ts(MyDateUtil.toTs(jsonObj.getString("create_time")))
                        .comment_ct(1L)
                        .good_comment_ct(GmallConstant.APPRAISE_GOOD.equals(jsonObj.getString("appraise")) ? 1L : 0L)
                        .build();
                }
            }
        );
        //TODO : 合并流
        DataStream<ProductStats> unionDS = clickAndDisplayStatsDS.union(
            orderWideStatsDS, paymentStatsDS, cartInfoStatsDS, favorInfoStatsDS, refundInfoStatsDS, commentInfoStatsDS
        );
        //TODO : 设置事件时间
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );
        //TODO : 分流
        KeyedStream<ProductStats, Long> productStatsKeyedDS =
            productStatsWithWatermarkDS.keyBy(r -> r.getSku_id());
        //TODO : 开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowDS =
            productStatsKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO : 聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
            new ReduceFunction<ProductStats>() {
                @Override
                public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                    stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                    stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                    stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                    stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                    stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                    //集合之间的相互添加
                    stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                    stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);

                    stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                    stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                    stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                    stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                    stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                    stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                    stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                    stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                    stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                    return stats1;
                }
            },
            new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                @Override
                public void process(Long aLong, Context context, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                    for (ProductStats productStats : iterable) {
                        productStats.setStt(MyDateUtil.toYMDhms(new Date(context.window().getStart())));
                        productStats.setEdt(MyDateUtil.toYMDhms(new Date(context.window().getEnd())));
                        collector.collect(productStats);
                    }
                }
            }
        );
        //TODO : 维度关联
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
            reduceDS,
            new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSku_id().toString();
                }

                @Override
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) {
                    productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                    productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                    productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                    productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                }
            },
            60, TimeUnit.SECONDS
        );
        //9.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
            AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) {
                        productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);


        //9.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
            AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject){
                        productStats.setCategory3_name(jsonObject.getString("NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //9.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
            AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) {
                        productStats.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        productStatsWithTmDstream.print("ProductStats>>>>>>>");
        //TODO : Sink到Clickhouse
        String sql = "insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        productStatsWithTmDstream.addSink(ClickHouseUtil.getJdbcSink(sql));
        env.execute();
    }
}
