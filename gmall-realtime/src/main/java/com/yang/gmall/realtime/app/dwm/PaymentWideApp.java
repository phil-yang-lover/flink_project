package com.yang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.yang.gmall.realtime.bean.OrderWide;
import com.yang.gmall.realtime.bean.PaymentInfo;
import com.yang.gmall.realtime.bean.PaymentWide;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 设置并行度
        env.setParallelism(4);
        //1.2 设置检查点
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend(""));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO 2.从kafka中读取数据
        String groupId = "paymentwide_app";
        //2.1 订单宽表数据(dwm_order_wide)
        String dwmOrderWideTopic = "dwm_order_wide";
        FlinkKafkaConsumer<String> orderWideKafkaStr =
            MyKafkaUtil.getKafkaSource(dwmOrderWideTopic, groupId);
        DataStreamSource<String> orderWideSource = env.addSource(orderWideKafkaStr);
        //2.2 支付表数据(dwd_payment_info)
        String dwdPaymentInfoTopic = "dwd_payment_info";
        FlinkKafkaConsumer<String> dwdPaymentInfoKafkaStr =
            MyKafkaUtil.getKafkaSource(dwdPaymentInfoTopic, groupId);
        DataStreamSource<String> dwdPaymentInfoSource = env.addSource(dwdPaymentInfoKafkaStr);

        //TODO 3.对数据进行结构的转换
        //3.1 订单宽表数据  jsonStr->OrderWide
        SingleOutputStreamOperator<OrderWide> orderWideJsonObj =
            orderWideSource.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));
        //3.2 支付表数据  jsonStr->PaymentInfo
        SingleOutputStreamOperator<PaymentInfo> paymentJsonObj =
            dwdPaymentInfoSource.map(json -> JSON.parseObject(json, PaymentInfo.class));

        //TODO 4.设置Watermark以及提取事件时间字段
        //4.1 订单宽表(创建时间ts)
        SingleOutputStreamOperator<OrderWide> orderWideJsonObjWithTime = orderWideJsonObj.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        String create_time = element.getCreate_time();
                        long ts = Timestamp.valueOf(create_time).getTime();
                        System.out.println("=====================");
                        System.out.println(">>>>>>>>"+ts);
                        return ts;
                    }
                })
        );
        //4.2 支付表(回调时间ts)
        SingleOutputStreamOperator<PaymentInfo> paymentJsonObjWithTime = paymentJsonObj.assignTimestampsAndWatermarks(
            WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        return Timestamp.valueOf(element.getCallback_time()).getTime();
                    }
                })
        );

        //TODO 5.按照order_id对流中数据进行分组(指定双流join字段)
        //4.3 订单宽表(order_id)
        KeyedStream<OrderWide, Long> orderWideJsonObjDS =
            orderWideJsonObjWithTime.keyBy(orderId -> orderId.getOrder_id());
        //4.3 支付表(order_id)
        KeyedStream<PaymentInfo, Long> paymentInfoJsonObjDS =
            paymentJsonObjWithTime.keyBy(orderId -> orderId.getOrder_id());

        //TODO 6.双流join(intervalJoin)  a.intervalJoin(b).between.process
        KeyedStream.IntervalJoin<PaymentInfo, OrderWide, Long> paymentInfoOrderWideLongIntervalJoin =
            paymentInfoJsonObjDS.intervalJoin(orderWideJsonObjDS);

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoOrderWideLongIntervalJoin
            .between(Time.seconds(-1800), Time.seconds(0))
            .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                @Override
                public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                    collector.collect(new PaymentWide(paymentInfo,orderWide));
                }
            });

        //TODO 7.将join之后的数据写回到kafka的dwm_payment_wide
        SingleOutputStreamOperator<String> dwmPaymentWideJsonStrDS = paymentWideDS
            .map(jsonStr -> JSON.toJSONString(jsonStr));

        dwmPaymentWideJsonStrDS.print();

        //dwmPaymentWideJsonStrDS.addSink(MyKafkaUtil.getKafkaSink("dwm_payment_wide"));


        env.execute();
    }
}
