package com.nov.kafka.producer;

import java.util.concurrent.*;

/**
 * @author november
 */
public class TestFuture {
    /**
     * 模拟producerAPI同步发送
     * 线程池
     */
    private static ExecutorService executorService=new ThreadPoolExecutor(10,10,60L,TimeUnit.SECONDS,new ArrayBlockingQueue<>(10));

    private static final int NUM=100;

    public static void main(String[] args) throws Exception {
        Future<?> submit = executorService.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < NUM; i++) {
                    System.out.println("i=" + i);
                }
            }
        });

        //阻塞，会在线程执行完后，主线程继续执行
        submit.get();

        System.out.println("=======================");

        executorService.shutdown();
    }
}
