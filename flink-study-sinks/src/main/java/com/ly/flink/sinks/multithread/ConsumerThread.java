package com.ly.flink.sinks.multithread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ConsumerThread implements Runnable{
    private Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    private LinkedBlockingQueue<String> buffer;
    private CyclicBarrier barrier;

    public ConsumerThread(LinkedBlockingQueue<String> buffer, CyclicBarrier barrier) {
        this.buffer = buffer;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        String data;
        while (true){
            try {
                data = buffer.poll(50, TimeUnit.MILLISECONDS);
                if (data != null) {
                    consume(data);
                } else {
                    // data ！= null & barrier wait > 0 表明当前正在进行checkpoint
                    if (barrier.getNumberWaiting() > 0) {
                        LOG.info("ConsumerThread start to flush, " +
                                "current waiting thread's number is：" + barrier.getNumberWaiting());
                        flush();
                        // 等待所有ConsumerThread flush 完成
                        barrier.await();
                    }
                }
            }catch (InterruptedException| BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    private void consume(String entity) {
        // 实际的业务消费逻辑
        // 消费异常需要考虑是否将数据重新放入buffer，但是即使重新放入也有其他风险，比如数据乱序
    }

    // flush，防止丢数据
    private void flush() {
        // client.flush();
    }
}
