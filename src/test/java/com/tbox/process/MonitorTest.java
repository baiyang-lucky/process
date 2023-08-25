package com.tbox.process;

import com.tbox.process.support.MasterExecutor;
import com.tbox.process.support.ExecutorContext;
import com.tbox.process.support.ExecutorProperties;
import com.tbox.process.support.GlobalExecutorRegistryAgent;
import com.tbox.process.type.LimitVelocityStrategy;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MonitorTest {
    public static void main(String[] args) throws InterruptedException {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ExecutorContext maxPressure = GlobalExecutorRegistryAgent.findMaxPressure();
                ExecutorContext minPressure = GlobalExecutorRegistryAgent.findMinPressure();
                String maxName = maxPressure != null ? maxPressure.getName() : "N/A";
                String minName = minPressure != null ? minPressure.getName() : "N/A";
                int maxSize = maxPressure != null ? maxPressure.getQueue().size() : 0;
                int minSize = minPressure != null ? minPressure.getQueue().size() : 0;

                System.out.printf("------------------------------------------------\n" +
                                "所有状态对应数量:" + GlobalExecutorRegistryAgent.getAllStateCount() + "\n" +
                                "处理压力最大/最小: %s:%d/%s:%d%n \n",
                        maxName, maxSize, minName, minSize);
            }
        }).start();
        ArrayList<MasterExecutor<String>> tasks = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1000);
        for (int i = 0; i < 5; i++) {
            tasks.add(createTask("task" + i, ExecutorProperties.builder()
//                    .limitVelocityStrategy(LimitVelocityStrategy.NO_LIMIT)
                    .limitVelocityStrategy(LimitVelocityStrategy.BALANCE)
                    .queueSize(2048)
//                    .coreWorkerSize(1 + i / 2)
                    .coreWorkerSize(1)
                    .maxWorkerSize(5)
                    .build(), countDownLatch));
        }
        long s = System.nanoTime();
        for (MasterExecutor<String> task : tasks) {
//            Thread.sleep(1000);
            task.start();
        }
        countDownLatch.await();
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - s));
        for (MasterExecutor<String> task : tasks) {
            Thread.sleep(1000);
            task.shutdownNow();
        }
        Thread.sleep(3000);
//        for (MsterExecutor<String> task : tasks) {
//            Thread.sleep(1000);
//            task.start();
//        }
//        Iterator<MsterExecutor<String>> iterator = tasks.iterator();
//        while (iterator.hasNext()) {
//            MsterExecutor<String> task = iterator.next();
//            Thread.sleep(1000);
//            task.shutdown();
//            iterator.remove();
//            System.gc();
//        }
        Thread.sleep(1000 * 60);
    }

    public static MasterExecutor<String> createTask(String name, ExecutorProperties properties, CountDownLatch countDownLatch) {
        return new MasterExecutor.Builder<String>()
                .name(name)
                .executorProperties(properties)
                .masterPuller(() -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    ArrayList<String> arr = new ArrayList<>();
                    arr.add("hello");
                    return arr;
                })
                .workerConsumer((data) -> {
                    try {
//                        Thread.sleep(new Random().nextInt(50));
                        Thread.sleep(100);
                        countDownLatch.countDown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();
    }
}
