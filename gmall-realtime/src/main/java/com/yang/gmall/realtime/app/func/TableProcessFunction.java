package com.yang.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.bean.TableProcess;
import com.yang.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    //
    Connection conn;
    //状态描述器
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    //将测输出流变量引入，保存维度信息
    OutputTag<JSONObject> dimOutputTag;

    public TableProcessFunction(OutputTag<JSONObject> dimOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimOutputTag = dimOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        System.out.println("连接phoenix成功");
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //TODO : 从广播状态里面先获取配置信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //TODO : 获取业务数据的关键字段
        String tableName = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        //注意：如果使用maxwell的bootstrap接收历史数据的时候，接收的类型是bootstrap-insert
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObject.put("type", type);
        }
        //生成和配置表中相同的key
        String key = tableName + ":" + type;
        //TODO : 匹配广播数据和业务数据。
        // 分流 并且用配置表里面新一层的数据 更新 输出的JSONObject
        TableProcess tableProcess = broadcastState.get(key);
        //如果配置表里有相同的key则进行 更新 分流
        if (tableProcess!=null){
            //更改表名
            jsonObject.put("sink_table",tableProcess.getSinkTable());
            //根据配置表的字段过滤业务数据的字段
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            if (sinkColumns!=null&&sinkColumns.length()>0){
                filterColumn(dataJsonObj,sinkColumns);
            }
            //分流
            if(tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //维度表   放到维度侧输出流
                readOnlyContext.output(dimOutputTag,jsonObject);
            }else if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                collector.collect(jsonObject);
            }
        }else {
            System.out.println("No this Key in TableProcess:" + key);
        }
    }
    //根据配置表中的sinkColumn配置，对json对象中的数据进行过滤，将没有在配置表中出现的字段过滤掉
    //流中数据：{"id":XX,"tm_name":"","log_url"}  ------------配置表中sinkColumn  id,tm_name
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(ele->!fieldList.contains(ele.getKey()));
    }

    //TODO : 处理配置表的信息，是hbase的信息进行提前建表，Kafka的不需要提前建分区等工作
    //     注意两个流只有键一致的时候才能连接。
    //     keyBy 的作用是将流数据分区，当 keyed stream 被连接时，他们必须按相同的方式分区。
    //     这样保证了两个流中所有键相同的事件发到同一个实例上。这样也使按键关联两个流成为可能。
    //    在这个例子中，两个流都是 DataStream<String> 类型的，并且都将字符串作为键。
    //               并且在processBroadcastElement方法中工作：
    //               1.对数据进行类的映射
    //               2.对配置表里面的数据做提前区分和对业务数据相关的操作准备环境（建表）
    //               3.整理出和业务数据进行连接的 key ,进行写状态，给业务数据流进行匹配
    //   注意：因为在创建配置信息的广播流的时候就需要提前创建状态管理描述器，需要传递进来
    @Override
    public void processBroadcastElement(String jsonString, Context context, Collector<JSONObject> collector) throws Exception {
        //TODO : 1.对数据进行类的映射
        //将字符串转换成JSON对象
        JSONObject jsonObject = JSON.parseObject(jsonString);
        //使用getString是因为JSON转成实体类的方法只支持传递字符串
        String datajsonStr = jsonObject.getString("data");
        //JSON类可以直接转成自定义实体类
        TableProcess tableProcess = JSON.parseObject(datajsonStr, TableProcess.class);

        if (tableProcess!=null){
            //获取业务数据库表名
            String sourceTable = tableProcess.getSourceTable();
            //获取操作类型
            String operateType = tableProcess.getOperateType();
            //hbase|kafka  用于标记是维度还是事实
            String sinkType = tableProcess.getSinkType();
            //获取输出到目的地的表名或者主题名
            String sinkTable = tableProcess.getSinkTable();
            //获取输出的列
            String sinkColumns = tableProcess.getSinkColumns();
            //主键
            String sinkPk = tableProcess.getSinkPk();
            //建表扩展
            String sinkExtend = tableProcess.getSinkExtend();

            //TODO : 2.在Hbase建表--利用phoenix
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)&&"insert".equals(operateType)){
                checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
            }

            //TODO : 3.整理出和业务数据进行连接的 key ,进行写状态，给业务数据流进行匹配
            //拼接状态中的key
            String key = sourceTable + ":" + operateType;
            //将配置信息放到状态中
            context.getBroadcastState(mapStateDescriptor).put(key,tableProcess);
        }
    }
    //TODO : 利用phoenix 1. 连接
    //                   2. 拼接SQL
    //                   3. 执行SQL
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //TODO : 设置主键
        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }
        //TODO : 拼接建表语句
        StringBuilder createSql = new StringBuilder(
            "create table if not exists " +GmallConfig.HBASE_SCHEMA+
                "." + sinkTable + "(");

        //根据配置表里面存放列字段的值获取建表的字段
        String[] fieldsArr = sinkColumns.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if (field.equals(sinkPk)){
                createSql.append(field+" varchar primary key");
            }else{
                createSql.append(field+" varchar");
            }
            if (i<fieldsArr.length-1){
                createSql.append(",");
            }
        }
        createSql.append(")"+sinkExtend);
        System.out.println("建表语句： "+createSql);
        //TODO : 连接phoenix数据库,并执行
        // connection
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在phoenix中建表失败");
        }finally {
            if (ps!=null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}





