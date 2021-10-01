package com.yang.gmall.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class FlinkCDC_01_DS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop2101:9820/gmall/checkpoint"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
            .hostname("hadoop2101")
            .port(3306)
            .databaseList("gmall-flink2021realtime") // monitor all tables under inventory database
            .tableList("gmall-flink2021realtime.test")
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            //.deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
            .deserializer(new MyDeserializationSchema()) // converts SourceRecord to String
            .build();

        env
            .addSource(sourceFunction)
            .print();

        env.execute();
    }

    private static class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //定义一个json对象，封装整体返回的结果
            JSONObject resJsonObj = new JSONObject();
            Struct value = (Struct) sourceRecord.value();
            Struct sourceStruct = (Struct) value.get("source");
            String db = sourceStruct.get("db").toString();
            String table = sourceStruct.get("table").toString();

            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)){
                type = "insert";
            }
            //System.out.println();

            Struct afterValue = (Struct) value.get("after");
            JSONObject jsonValue = new JSONObject();
            if (afterValue != null) {
                for (Field field : afterValue.schema().fields()) {
                    jsonValue.put(field.name(), afterValue.get(field));
                }
            }
            resJsonObj.put("database", db);
            resJsonObj.put("table", table);
            resJsonObj.put("type", type);
            resJsonObj.put("data", jsonValue);
            //将反序列化的结果 写出
            collector.collect(resJsonObj.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
