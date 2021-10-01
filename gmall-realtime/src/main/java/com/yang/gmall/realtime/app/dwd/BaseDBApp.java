package com.yang.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.yang.gmall.realtime.app.func.DimSink;
import com.yang.gmall.realtime.app.func.MyDeserializationSchemaFunction;
import com.yang.gmall.realtime.app.func.TableProcessFunction;
import com.yang.gmall.realtime.bean.TableProcess;
import com.yang.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop2101:9820/xxx"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                boolean b = value.getString("table") != null
                    && value.getString("table").length() > 0
                    && value.getJSONObject("data") != null
                    && value.getString("data").length() > 3;
                return b;
            }
        });

        filterDS.print();

        DebeziumSourceFunction mysqlCDC = MySQLSource.<String>builder()
            .hostname("hadoop2101")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall-flink2021realtime")
            .tableList("gmall-flink2021realtime.table_process")
            .deserializer(new MyDeserializationSchemaFunction())
            .startupOptions(StartupOptions.initial())
            .build();

        env.addSource(mysqlCDC).print();
        DataStreamSource mysqlDS = env.addSource(mysqlCDC);

        //TODO : 上面获得两条流，开始利用配置表将动态变化的业务数据进行区分存储
        //       将配置表用flinkCDC读取到以广播状态流的形式和业务数据流连接处理
        //将配置表获取的json字符串以流的形式广播
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
            "mapStateDescriptor", String.class, TableProcess.class
        );
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);

        //将广播流和业务数据进行连接
        //`connect()` 方法需要由非广播流来进行调用，`BroadcastStream` 作为参数传入。
        OutputTag<JSONObject> dimOutputTag = new OutputTag<JSONObject>("dimTag"){};
        BroadcastConnectedStream<JSONObject,String> connectDS = filterDS.connect(broadcastDS);

        //TODO : 处理连接流
        SingleOutputStreamOperator<JSONObject> splitDS = connectDS.process(new TableProcessFunction(dimOutputTag,mapStateDescriptor));
        //获取测输出流
        DataStream<JSONObject> dimDS = splitDS.getSideOutput(dimOutputTag);

        splitDS.print("主：splitDS====");
        dimDS.print("侧：dimDS=====");

        //TODO : 处理dimDS测输出流----phoenix(hbase)
        dimDS.addSink(new DimSink());

        //TODO : 处理splitDS到kafka
        splitDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                String topic = element.getString("sink_table");
                JSONObject dataJsonObj = element.getJSONObject("data");
                return new ProducerRecord<byte[], byte[]>(topic,dataJsonObj.toJSONString().getBytes());
            }
        }));

    env.execute();
    }
}
