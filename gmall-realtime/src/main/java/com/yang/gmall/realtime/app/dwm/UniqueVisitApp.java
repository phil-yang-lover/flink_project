package com.yang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

//TODO : 还没有统计日活，只是将符合日独立访客的信息筛选出来保存到当天的独立访客信息表dwm_unique_visit中
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend(""));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        String topic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //TODO : 读取dwd层的page_log数据计算UV
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        jsonObjDS.print(">>>>>");

        //TODO : 从dwd层
        //       1. 根据是否有上一次登录页面判断当天第一次登录的访客
        //             如果是当天第一次登录的访客，用 状态(open方法里) 保存访问时间，是否已经记录过

        //TODO : 先将dwd_page_log中的访客按mid分组
        KeyedStream<JSONObject, String> keyedDS =
            jsonObjDS.keyBy(mid -> mid.getJSONObject("common").getString("mid"));

        //TODO : 过滤出当天第一次登录的访客
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {
            //初始化状态保存上一次访问的时间
            ValueState<String> lastVisitDateState;
            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyyMMdd");
                //因为是日活（一天内第一次登录的访客），所以状态不需要常驻内存，可以设置状态的存活时间
                //对状态的设置就是通过对状态描述器配置
                ValueStateDescriptor valueStateDescriptor =
                    new ValueStateDescriptor("valueStateDescriptor", String.class);

                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1L))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                //第一次登录的访客---再判断过滤去重
                //获取状态中的上次访问日期

                //获取日志数据中的当前访问日期
                Long ts = value.getLong("ts");
                String curDate = sdf.format(ts);

                if (lastPageId != null && lastPageId.length() > 0 && lastPageId.equals(curDate)) {
                    //已经访问过了
                    return false;
                } else {
                    lastVisitDateState.update(curDate);
                    return true;
                }
            }
        });

        //TODO : 转成String写进kafka的dwm层
        DataStreamSink<String> dwm_unique_visitDS = filterDS
            .map(jsonObj -> jsonObj.toJSONString())
            .addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        env.execute();
    }
}
