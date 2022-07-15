package com.ly.flink.sql.stream.join;

import com.ly.flink.sql.common.SQLFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamIntervalJoinExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        tEnv.executeSql(SQLFactory.clickDDL);
        tEnv.executeSql(SQLFactory.orderDDL);
        String sql = "SELECT * FROM click LEFT JOIN orders ON click.userid = orders.userid " +
                " and click.proctime >= orders.proctime" +
                " and  click.proctime < orders.proctime + INTERVAL '10' MINUTE";

        System.out.println(tEnv.explainSql(sql));
        // tEnv.executeSql(sql);
    }
}
