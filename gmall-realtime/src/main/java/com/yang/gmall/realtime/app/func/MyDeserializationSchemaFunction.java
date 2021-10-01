package com.yang.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        Struct value = (Struct) sourceRecord.value();
        Struct sourceStruct = (Struct) value.get("source");
        Struct afterStruct = (Struct) value.get("after");

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        String db = sourceStruct.get("db").toString();
        String table = sourceStruct.get("table").toString();

        JSONObject afterObject = new JSONObject();
        if (afterStruct != null) {
            for (Field field : afterStruct.schema().fields()) {
                afterObject.put(field.name(), afterStruct.get(field));
            }
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("database", db);
        jsonObject.put("table", table);
        jsonObject.put("type", type);
        jsonObject.put("data", afterObject);

        collector.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }
}
