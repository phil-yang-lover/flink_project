package com.yang.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
//TODO : 创建连接HBASE查询数据的工具类：由上一层DimUtil提供SQL
public class MyPhoenixUtil {
    private static Connection conn;
    public static <T>List<T> queryList(String sql,Class<T> clz){
        //TODO : 连接Phoenix
        if (conn == null){
            initConnection();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> resList = new ArrayList<>();
        //TODO : 处理结果集
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    BeanUtils.setProperty(obj,columnName,rs.getObject(i));
                }
                resList.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (rs!=null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    private static void initConnection() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        List<JSONObject> list = queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class);
        System.out.println(list);
    }
}
