package com.yang.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class MyPatternProcessFunction extends PatternProcessFunction<JSONObject, String> implements TimedOutPartialMatchHandler<JSONObject> {
    OutputTag<String> outputTag;
    public MyPatternProcessFunction(OutputTag<String> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processMatch(Map<String, List<JSONObject>> map, Context context, Collector<String> collector) throws Exception {

    }

    @Override
    public void processTimedOutMatch(Map<String, List<JSONObject>> map, Context context) throws Exception {
        JSONObject jsonObject = map.get("first").get(0);
        context.output(outputTag, jsonObject.toJSONString());
        /*List<JSONObject> jsonObjectList = map.get("first");
        for (JSONObject jsonObj : jsonObjectList) {
            //继续将日志数据向下游输出
            context.output(outputTag,jsonObj.toJSONString());
        }*/
    }
}
