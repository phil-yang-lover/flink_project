package com.yang.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UVApp {
    //读取kafka里面的dwd_page_id
    //当天第一次登录的访客
    //（日志采集的就是当天的）根据是否有上一个的登录页面，当天的用状态存的时间进行判断是否已经来过了
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        /*env
            .enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
            .getCheckpointConfig().setCheckpointTimeout(60000L);
        env
            .setStateBackend(new FsStateBackend(""))
            .setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        String topic = "dwd_page_id";
        String groupId = "uv_group";
        DataStreamSource<String> uvSource =
            env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));
        //转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjDS = uvSource.map(JSON::parseObject);
        //过滤前要分组
        //分组
        KeyedStream<JSONObject, String> keyedDS =
            jsonObjDS.keyBy(mid -> mid.getJSONObject("page").getString("mid"));
        //过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {
            //注意：因为是日活所以状态里面用户的时间要设置有效期1天
            SimpleDateFormat sdf;
            ValueState<String> lastDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor("lastdate", Long.class);

                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1L))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

                valueStateDescriptor.enableTimeToLive(ttlConfig);
                lastDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //先判断是否有上一个页面
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                //再判断状态里面是否已经存在当天的访问记录
                String lastVisit = lastDateState.value();
                Long ts = value.getLong("ts");
                String curDate = sdf.format(ts);
                if (lastVisit != null && lastVisit.length() > 0 && lastVisit.equals(curDate)) {
                    return false;
                } else {
                    lastDateState.update(curDate);
                    return true;
                }
            }
        });
        SingleOutputStreamOperator<String> jsonStrDS = filterDS.map(json->json.toJSONString());

        jsonStrDS.addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        env.execute();
    }
}
