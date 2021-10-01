package com.yang.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.yang.gmall.realtime.common.GmallConfig;
import com.yang.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//TODO : 因为要连接phoenix初始化所以要open执行一次连接用富函数
public class DimSink extends RichSinkFunction<JSONObject> {
    Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String tableName = value.getString("sink_table");
        //TODO : 获取数据生成 SQL
        JSONObject dataJsonObject = value.getJSONObject("data");
        if (dataJsonObject != null && dataJsonObject.size() > 0) {
            String upsertSql = genUpsertSql(tableName,dataJsonObject);

            //创建数据库操作对象
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(upsertSql);
                ps.execute();
                //注意：Phoenix需要手动提交事务
                conn.commit();
                System.out.println("执行的SQL语句为:" + upsertSql);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向Phoenix中插入数据失败");
            } finally {
                if(ps!=null){
                    ps.close();
                }
            }
        }//上面的程序不管是插入还是更新或删除都操作了HBASE，下面是对缓存里面的进行删除或修改
        //TODO : 上面HBASE插入失败直接就报异常了，不往下执行了
        if (value.getString("type").equals("update") ||
            value.getString("type").equals("delete")){
            DimUtil.deleteCached(tableName,dataJsonObject.getString("id"));
        }
    }

    private String genUpsertSql(String tableName, JSONObject dataJsonObject) {
        String upsertSql = "upsert into "+
            GmallConfig.HBASE_SCHEMA+"."+tableName+
            "("+ StringUtils.join(dataJsonObject.keySet(),",")+
            ") values('"+StringUtils.join(dataJsonObject.values(),"','")+"')";
        return upsertSql;
    }
}
