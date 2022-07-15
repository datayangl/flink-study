package com.ly.flink.sql.stream.join;

import com.ly.flink.sql.common.SQLFactory;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class StreamRegularJoinExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
        // note：regular join 的状态会存储双流的所有数据，因此需要设置 ttl 避免状态一直增长
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(1));
        //tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "1H");

        tEnv.executeSql(SQLFactory.clickDDL);
        tEnv.executeSql(SQLFactory.orderDDL);

        String sql = "SELECT * FROM click LEFT JOIN orders ON click.userid = orders.userid";
        System.out.println(tEnv.explainSql(sql));
        // tEnv.executeSql(sql);
    }
}
