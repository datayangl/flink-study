package com.ly.flink.connectors.redis;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;

public class RedisSinkFunction extends RichSinkFunction<RowData> {
    private final String redisHost;
    private final int redisPort;
    private Jedis jedis;

    public RedisSinkFunction(ReadableConfig options) {
        this.redisHost = options.getOptional(RedisDynamicTableFactory.REDIS_HOST).get();
        this.redisPort = options.getOptional(RedisDynamicTableFactory.REDIS_PORT).get();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis(redisHost, redisPort);
    }

    /**
     * 访问redis，完成操作
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(RowData value, Context context) throws Exception {
        byte[] k = value.getString(1).toString().getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getString(2).toString().getBytes(StandardCharsets.UTF_8);

        Jedis jedis = null;
        try {
            jedis.set(k, v);
        } catch (Exception e) {
            // 异常处理
        }
    }

    @Override
    public void close() throws Exception {
        if (jedis != null ){
            jedis.close();
        }
    }
}
