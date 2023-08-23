package com.tbox.process;

import com.tbox.process.support.ControllableMsterExecutor;
import com.tbox.process.support.ExecutorProperties;

import java.util.ArrayList;
import java.util.Random;

public class ProcessTest {
    public static void main(String[] args) throws InterruptedException {

        ControllableMsterExecutor<String> printTask = new ControllableMsterExecutor.Builder<String>()
                .name("print_task")
                .executorProperties(ExecutorProperties.builder()
                        .queueSize(200)
                        .coreWorkerSize(1)
                        .maxWorkderSize(5)
                        .build())
                .masterPuller(() -> {
                    ArrayList<String> books = new ArrayList<>();
                    books.add("baiyang");
//                    System.out.println(Thread.currentThread().getName() + "拉取数据...");
                    return books;
                })
                .workerConsumer((data) -> {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
//                    System.out.println(Thread.currentThread().getName() + ":" + data);
                })
                .build();
        printTask.start();
        Thread.sleep(1000);
        printTask.stop();
        Thread.sleep(5000);
        printTask.start();
        Thread.sleep(1000);
        printTask.stopNow();
        Thread.sleep(1000);
        printTask.start();
        Thread.sleep(1000);
        printTask.shutdown();
        Thread.sleep(5000);
    }
}

