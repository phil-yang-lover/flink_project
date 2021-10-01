package com.yang.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.temporal.ValueRange;
import java.util.Properties;

//TODO : 对日志数据进行处理，（确定（修复）好新老用户）整理后写入下游的kafka的dwd---topic
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO : 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop2101:9820/gmall/checkpoint"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/

        //TODO : 从kafka的ods_base_log中读取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        //TODO : 将数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //TODO : 用状态对是否是新老用户的值进行修复--首先按（mid）用户分区；其次对mid的状态判断
        SingleOutputStreamOperator<JSONObject> newJsonObjDS = jsonObjDS
            .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
            .map(new RichMapFunction<JSONObject, JSONObject>() {
                //如果用每个mid来保存状态，不能判断当天新用户多次登录使用，所以使用时间
                private ValueState<String> midFirstVisitDateTime;
                SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyyMMdd");
                    midFirstVisitDateTime = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                        "midFirstVisitDateTime", Types.STRING
                    ));
                }

                @Override
                public JSONObject map(JSONObject value) throws Exception {
                    String isNew = value.getJSONObject("common").getString("is_new");
                    if ("1".equals(isNew)) {
                        //
                        String ts = sdf.format(value.getLong("ts"));
                        //判断mid是否已经存在//mid存在判断是否是注册当天的多次登录，判断时间
                        if (midFirstVisitDateTime.value() != null && !midFirstVisitDateTime.value().equals(ts)) {
                            isNew = "0";
                            value.getJSONObject("common").put("is_new", isNew);
                        } else {
                            midFirstVisitDateTime.update(ts);
                        }
                    }
                    return value;
                }
            });

        //TODO : 利用侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> resultDS = newJsonObjDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                JSONObject startObj = jsonObject.getJSONObject("start");
                if (startObj != null && startObj.size() > 0) {
                    context.output(startTag, startObj.toJSONString());
                } else {
                    collector.collect(jsonObject.toJSONString());
                    JSONArray displaysArr = jsonObject.getJSONArray("displays");
                    if (displaysArr != null && displaysArr.size() > 0) {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displaysArr.size(); i++) {
                            JSONObject displaysArrJSONObj = displaysArr.getJSONObject(i);
                            displaysArrJSONObj.put("page_id", pageId);
                            context.output(displayTag, displaysArrJSONObj.toJSONString());
                        }
                    }
                }
            }
        });
        resultDS.getSideOutput(startTag).print("startDS>>>>>>");
        resultDS.getSideOutput(displayTag).print("displaysDS>>>>>>>>");
        resultDS.print("main>>>>>>>>");

        //TODO : 下沉到kafka
        resultDS.getSideOutput(startTag).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        resultDS.getSideOutput(displayTag).addSink(MyKafkaUtil.getKafkaSink("dwd_displays_log"));
        resultDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));

        env.execute();
    }
}
