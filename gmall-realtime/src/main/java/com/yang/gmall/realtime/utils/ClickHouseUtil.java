package com.yang.gmall.realtime.utils;

import com.yang.gmall.realtime.bean.TransientSink;
import com.yang.gmall.realtime.bean.VisitorStats;
import com.yang.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T>SinkFunction getJdbcSink(String sql){
        SinkFunction<T> sinkFunction = JdbcSink.sink(
            sql,
            new JdbcStatementBuilder<T>() {
                @Override
                public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                    // getDeclaredFields()：
                    // 获得某个类的所有声明的字段，即包括public、private和proteced，但是不包括父类的申明字段。
                    Field[] declaredFields = t.getClass().getDeclaredFields();
                    //使用注解排除不插入的字段
                    int skipNum = 0;
                    for (int i = 0; i < declaredFields.length; i++) {
                        Field declaredField = declaredFields[i];
                        //判断属性是否要保存到click house中
                        TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                        if (transientSink!=null){
                            System.out.println("跳过属性" + declaredField.getName());
                            skipNum++;
                            continue;
                        }
                        declaredField.setAccessible(true);
                        try {
                            preparedStatement.setObject(i+1-skipNum,declaredField.get(t));
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
            },
            new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(GmallConfig.CLICKHOUSE_URL)
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .build()
        );
        return sinkFunction;
    }
}
