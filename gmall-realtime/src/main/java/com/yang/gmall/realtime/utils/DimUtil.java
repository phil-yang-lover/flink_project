package com.yang.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

//TODO : 获得查询HBASE的SQL语句
//       1.直接查询HBASE
//       2.使用Redis缓存旁路查询
//         随数据的变化更新缓存（先更新HBASE再删除Redis）
public class DimUtil {
    //  TODO     1.直接查询HBASE
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValues) {
        String dimSql = "select * from " + tableName + " where ";
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columeName = columnNameAndValue.f0;
            String columeValue = columnNameAndValue.f1;
            if (i > 0) {
                dimSql += " and ";
            }
            dimSql += columeName + "='" + columeValue + "' ";
        }
        System.out.println("维度查询的SQL:" + dimSql);
        List<JSONObject> dimInfoList = MyPhoenixUtil.queryList(dimSql, JSONObject.class);
        JSONObject dimInfoJsonObject=null;
        if (dimInfoList!=null && dimInfoList.size() > 0){
            dimInfoJsonObject = dimInfoList.get(0);
        }else{
            System.out.println("没有查询到维度数据:" + dimSql);
        }
        return dimInfoJsonObject;
    }

    //TODO : 指定一个条件
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName,Tuple2.of("id",id));
    }

        //  TODO     1.使用Redis缓存旁路查询----可以指定多个条件
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //TODO : 准备查询SQL语句 -- 先查Redis再查HBASE
        String dimSql = "select * from " + tableName + " where ";
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columeName = columnNameAndValue.f0;
            String columeValue = columnNameAndValue.f1;
            if (i > 0) {
                dimSql += " and ";
                redisKey += "_";
            }
            dimSql += columeName + "='" + columeValue + "' ";
            //Redis查询使用的key 无需把字段加上
            redisKey += columeValue;
        }
        //TODO : 查询
        Jedis jedis = null;
        String dimJsonStr = null;
        JSONObject dimInfoJsonObj = null;

        jedis = MyRedisUtil.getJedis();
        dimJsonStr = jedis.get(redisKey);

        if (dimJsonStr!=null && dimJsonStr.length() > 0){
            dimInfoJsonObj = JSON.parseObject(dimJsonStr);
        }else{
            System.out.println("维度查询的SQL:" + dimSql);
            List<JSONObject> dimInfoList = MyPhoenixUtil.queryList(dimSql, JSONObject.class);
            //JSONObject dimInfoJsonObject=null;
            if (dimInfoList!=null && dimInfoList.size() > 0){
                dimInfoJsonObj = dimInfoList.get(0);
                //查到后写进redis缓存
                if (jedis!=null){
                    jedis.setex(redisKey,3600*24,dimInfoJsonObj.toJSONString());
                }
            }else{
                System.out.println("没有查询到维度数据:" + dimSql);
            }
        }
        if (jedis!=null){
            jedis.close();
        }
        return dimInfoJsonObj;
    }

    public static void deleteCached(String tableName, String id) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":"+id;
        Jedis jedis = MyRedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) {
        JSONObject dimInfo =
            getDimInfo("DIM_BASE_TRADEMARK", "12");
        System.out.println(dimInfo);
        /*JSONObject dimInfoNoCache =
            getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "12"));
        System.out.println(dimInfoNoCache);*/
    }
}
