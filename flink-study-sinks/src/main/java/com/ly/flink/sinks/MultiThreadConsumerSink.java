package com.ly.flink.sinks;

import com.ly.flink.common.UdfConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MultiThreadConsumerSink extends RichSinkFunction<String> implements CheckpointedFunction {
    private LinkedBlockingQueue<String> buffer;
    private CyclicBarrier consumerBarrier;

    private Logger LOG = LoggerFactory.getLogger(MultiThreadConsumerSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        int consumerThreadNumber = parameters.getInteger(UdfConfigOptions.CONSUMER_THREAD_NUMBER);
        int bufferCapacity = parameters.getInteger(UdfConfigOptions.CONSUMER_BUFFER_CAPACITY);

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(consumerThreadNumber, consumerThreadNumber,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        this.buffer = new LinkedBlockingQueue(bufferCapacity);
        // CyclicBarrier 的等待线程数为 消费线程数 + 当前线程(即1)
        this.consumerBarrier = new CyclicBarrier(consumerThreadNumber + 1);

        ConsumerThread consumer= new ConsumerThread(buffer, consumerBarrier);
        for (int i=0; i < consumerThreadNumber; i++) {
            threadPoolExecutor.execute(consumer);
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        buffer.put(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        LOG.info("start to snapshot");
        consumerBarrier.await();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
