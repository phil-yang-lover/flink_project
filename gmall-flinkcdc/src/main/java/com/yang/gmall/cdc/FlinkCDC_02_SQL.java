package com.yang.gmall.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_02_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE user_info(" +
            " id INT," +
            " name STRING" +
            ") WITH (" +
            " 'connector' = 'mysql-cdc'," +
            " 'hostname' = 'hadoop2101'," +
            " 'port' = '3306'," +
            " 'username' = 'root'," +
            " 'password' = '123456'," +
            " 'database-name' = 'gmall-flink2021realtime'," +
            " 'table-name' = 'test'" +
            ")");
        tableEnv.executeSql("select * from user_info").print();

        env.execute();
    }
}
