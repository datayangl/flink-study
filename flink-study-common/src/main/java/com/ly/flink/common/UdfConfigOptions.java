package com.ly.flink.common;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * 自定义参数
 */
public class UdfConfigOptions {
    public static final ConfigOption<Integer> CONSUMER_THREAD_NUMBER =
            key("udf.consumer.thread.number")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The number of threads for multi thread consumer");

    public static final ConfigOption<Integer> CONSUMER_BUFFER_CAPACITY =
            key("udf.consumer.buffer.capacity")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The capacity of buffer");
}
