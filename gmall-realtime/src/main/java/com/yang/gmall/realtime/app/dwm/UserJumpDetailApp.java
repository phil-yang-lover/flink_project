package com.yang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.app.func.MyPatternProcessFunction;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

//TODO : 用户跳出明细统计
//      1.没有上一次的页面访问
//      2.没有下一次的页面
//          有下一次的，但是跳到下一页面的时间过长也是跳出
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        /*env
            .enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE)
            .getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend(""))
            .setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO ：读取kafka的dwd_page_log数据
        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //TODO ：本地测试数据
        /*DataStream<String> kafkaDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/

        //TODO : 对String转成JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //因为有下一个页面后，需要根据时间判断是否包含到跳出率中，所以引入每个页面处理的时间水位线
        //TODO : 提取事件中有关时间的字段，指定水位线策略
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })
        );

        //TODO : 分组
        KeyedStream<JSONObject, String> keyedDS =
            jsonObjWithWatermarkDS.keyBy(mid -> mid.getJSONObject("common").getString("mid"));

        //TODO : 定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    //根据page的ID判断所以获取lasr_page_id
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null || lastPageId.length() == 0) {
                        return true;
                    }
                    return false;
                }
            })
            .next("second").where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    //上一个页面不存在，获取当前页面page_id存在就返回真
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (pageId != null && pageId.length() > 0) {
                        return true;
                    }
                    return false;
                }
            }).within(Time.seconds(10));

        //TODO : 将模式应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        //TODO : 利用CEP的特性，处理超时、跳出的流
        //      1.select/flatSelect---旧版本
        //      2.PatternProcessFunction---Flink 1.8之后引入

        //  提取超时数据   ，对于我们的需求来说，超时数据就是跳出明细
        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        SingleOutputStreamOperator<String> timeOutDS =
            patternDS.process(new MyPatternProcessFunction(outputTag));

        DataStream<String> jumpDS = timeOutDS.getSideOutput(outputTag);

        jumpDS.print("dwm_user_jump_detail>>>>>>>>>>>>>>");
        jumpDS.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        env.execute();
    }
}
