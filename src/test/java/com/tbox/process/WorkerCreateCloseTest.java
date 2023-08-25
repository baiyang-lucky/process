package com.tbox.process;

import com.tbox.process.support.*;
import com.tbox.process.support.workercc.AccumulationRateWorkerCreateClose;
import com.tbox.process.type.LimitVelocityStrategy;

import java.util.ArrayList;
import java.util.Random;

public class WorkerCreateCloseTest {
    public static long PRD_VELOCITY = 10;

    public static void main(String[] args) throws InterruptedException {
        String taskName = "print_task";
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ExecutorContext excutorContext = GlobalExecutorRegistryAgent.findExcutorContext(taskName);
                if (excutorContext == null) {
                    continue;
                }
                System.out.printf("------------------------------------------------\n" +
                                "队列堆积压力:%d/%d , worker数量:%d/%d \n",
                        excutorContext.getQueue().size(), excutorContext.getExecutorProperties().getQueueSize(),
                        excutorContext.getWorkers().size(), excutorContext.getExecutorProperties().getMaxWorkerSize());
            }
        }).start();
        MasterExecutor<String> printTask = new MasterExecutor.Builder<String>()
                .name(taskName)
                .executorProperties(ExecutorProperties.builder()
                        .queueSize(1000) //队列长度
                        .coreWorkerSize(1) //核心worker数
                        .maxWorkerSize(5) //最大worker数
                        .workerAlivetime(5000) // woker空闲存活时间
                        .limitVelocityStrategy(LimitVelocityStrategy.NO_LIMIT)
                        .build())
//                .workerCreateClose(new AccumulationRateWorkerCreateClose(0.5, 0.2))
                .masterPuller(() -> { //Mster数据拉取
                    ArrayList<String> books = new ArrayList<>();
                    books.add("baiyang");
                    try {
                        Thread.sleep(PRD_VELOCITY);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return books;
                })
                .workerConsumer((data) -> { //Worker数据消费
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
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

