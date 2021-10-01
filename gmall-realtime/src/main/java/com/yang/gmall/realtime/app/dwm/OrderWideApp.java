package com.yang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.app.func.DimAsyncFunction;
import com.yang.gmall.realtime.bean.OrderDetail;
import com.yang.gmall.realtime.bean.OrderInfo;
import com.yang.gmall.realtime.bean.OrderWide;
import com.yang.gmall.realtime.utils.DimUtil;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.concurrent.TimeUnit;

//TODO : 订单宽表的双流join
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        /*env
            .enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
            .getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend(""))
            .setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO : 从kafka读取join的两张表
        String groupId = "orderwide_group";
        //订单
        String orderInfoTopic = "dwd_order_info";
        DataStreamSource<String> orderInfoSource =
            env.addSource(MyKafkaUtil.getKafkaSource(orderInfoTopic, groupId));
        //订单明细
        String orderDetailTopic = "dwd_order_detail";
        DataStreamSource<String> orderDetailSource =
            env.addSource(MyKafkaUtil.getKafkaSource(orderDetailTopic, groupId));

        //TODO : 将两条流数据进行结构转换
        //      1.映射成实体类
        //      2.时间的转换需要使用富函数的初始化
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoDS =
            (SingleOutputStreamOperator<OrderInfo>) orderInfoSource.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String value) throws Exception {
                //字符串转换成类，借助与JSON类再转换成实体类
                OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });
        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailSource.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //TODO : 指定提取事件字段和水位线
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                })
        );
        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                })
        );
        //TODO : 对两条流进行分别分组
        //订单
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS =
            orderInfoWithWatermarkDS.keyBy(mark -> mark.getId());
        //订单明细
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS =
            orderDetailWithWatermarkDS.keyBy(mark -> mark.getOrder_id());
        //TODO : 进行双流join
        //      1.开窗join有弊端
        //      2.使用间隔join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
            .intervalJoin(orderDetailKeyedDS)
            .between(Time.seconds(-5), Time.seconds(5))
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                @Override
                public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                    collector.collect(new OrderWide(orderInfo, orderDetail));
                }
            });

        orderWideDS.print("orderWide>>>>>>>>>>>");

        //TODO : 同步请求执行
        /*orderWideDS.map(new MapFunction<OrderWide, OrderWide>() {
            @Override
            public OrderWide map(OrderWide orderWide) throws Exception {
                Long user_id = orderWide.getUser_id();
                DimUtil.getDimInfo("DIM_BASE_TRADEMARK",user_id.toString());
                return orderWide;
            }
        });*/
        //TODO : 异步实现请求执行
        //TODO 7.用户维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
            orderWideDS,
            new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                @Override
                public String getKey(OrderWide orderWide) {
                    //return orderWide.getUser_id().toString();
                    return String.valueOf(orderWide.getUser_id());
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) {
                    String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                    LocalDate nowTime = LocalDate.now();
                    LocalDate birthtime = LocalDate.parse(birthday);
                    Period between = Period.between(birthtime, nowTime);
                    int age = between.getYears();
                    System.out.println("=======================================");
                    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>"+age);

                    orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                    orderWide.setUser_age(age);
                }
            }, 60, TimeUnit.SECONDS
        );
        orderWideWithUserDS.print();
        //TODO 8.关联省份维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
            orderWideWithUserDS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getProvince_id());
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) {
                    orderWide.setProvince_name(dimInfoJsonObj.getString("NAME"));
                    orderWide.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                    orderWide.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                    orderWide.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                }
            }, 60, TimeUnit.SECONDS
        );
        //TODO 9.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
            orderWideWithProvinceDS,
            new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getSku_id());
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) {
                    orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                    orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                    orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                }
            }, 60, TimeUnit.SECONDS
        );
        //TODO 10.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
            orderWideWithSkuDS,
            new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getSpu_id());
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) {
                    orderWide.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                }
            }, 60, TimeUnit.SECONDS
        );

        //TODO 11.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
            orderWideWithSpuDS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getCategory3_id());
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) {
                    orderWide.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                }
            }, 60, TimeUnit.SECONDS
        );
        //TODO 12.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
            orderWideWithCategory3DS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getTm_id());
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) {
                    orderWide.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                }
            }, 60, TimeUnit.SECONDS
        );
        orderWideWithTmDS.print(">>>>>>");
        //TODO 13.将订单宽表数据写回到kafka的dwm_order_wide
        String orderWideSinkTopic = "dwm_order_wide";
        orderWideWithTmDS.map(
            jsonStr->JSON.toJSONString(jsonStr)
        ).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        env.execute();
    }
}
