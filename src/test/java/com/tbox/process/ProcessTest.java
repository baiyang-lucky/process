package com.tbox.process;

import com.tbox.process.support.MasterExecutor;
import com.tbox.process.support.ExecutorProperties;

import java.util.ArrayList;
import java.util.Random;

public class ProcessTest {
    public static void main(String[] args) throws InterruptedException {

        MasterExecutor<String> printTask = new MasterExecutor.Builder<String>()
                .name("print_task")
                .executorProperties(ExecutorProperties.builder()
                        .queueSize(1024) //队列长度
                        .coreWorkerSize(1) //核心worker数
                        .maxWorkerSize(80) //最大worker数
                        .workerAlivetime(500) // woker空闲存活时间
                        .build())
                .masterPuller(() -> { //Mster数据拉取
                    ArrayList<String> books = new ArrayList<>();
                    books.add("baiyang");
                    try {
                        Thread.sleep(new Random().nextInt(1));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return books;
                })
                .workerConsumer((data) -> { //Worker数据消费
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();
        printTask.start();
        Thread.sleep(1000);
        printTask.stop();
        Thread.sleep(1000);
        printTask.start();
        Thread.sleep(1000);
        printTask.stopNow();
        Thread.sleep(1000);
        printTask.start();
        Thread.sleep(1000);
        printTask.shutdown();
//        Thread.sleep(5000);
    }
}

