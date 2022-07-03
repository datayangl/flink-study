package com.ly.flink.connectors.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import redis.clients.jedis.JedisCluster;

import java.util.*;

public class RedisDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "redis";

    public static final ConfigOption<String> REDIS_HOST = ConfigOptions.key("redis.host")
            .stringType()
            .noDefaultValue()
            .withDescription("redis host");

    public static final ConfigOption<Integer> REDIS_PORT =  ConfigOptions.key("redis.port")
            .intType()
            .noDefaultValue()
            .withDescription("redis port");

    public static final ConfigOption<String> REDIS_PASSWORD =  ConfigOptions.key("redis.password")
            .stringType()
            .noDefaultValue()
            .withDescription("redis password");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_HOST);
        options.add(REDIS_PORT);
        options.add(REDIS_PASSWORD);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new RedisDynamicTableSink(options);
    }

    public static class RedisDynamicTableSink implements DynamicTableSink {
        private ReadableConfig options;

        public RedisDynamicTableSink(ReadableConfig options) {
            this.options = options;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            RedisSinkFunction redisSinkFunction = new RedisSinkFunction(options);
            return SinkFunctionProvider.of(redisSinkFunction);
        }

        @Override
        public DynamicTableSink copy() {
            return new RedisDynamicTableSink(this.options);
        }

        @Override
        public String asSummaryString() {
            return "redis_factory";
        }
    }

}
