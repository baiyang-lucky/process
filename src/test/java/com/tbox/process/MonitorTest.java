package com.tbox.process;

import com.tbox.process.support.ControllableMsterExecutor;
import com.tbox.process.support.ExecutorContext;
import com.tbox.process.support.ExecutorProperties;
import com.tbox.process.support.GlobalExecutorRegistryAgent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

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
        ArrayList<ControllableMsterExecutor<String>> tasks = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            tasks.add(createTask("task" + i, ExecutorProperties.builder()
                    .queueSize(2048)
                    .coreWorkerSize(1 + i / 2)
                    .maxWorkderSize(1 + i)
                    .build()));
        }
        for (ControllableMsterExecutor<String> task : tasks) {
            Thread.sleep(1000);
            task.start();
        }
        for (ControllableMsterExecutor<String> task : tasks) {
            Thread.sleep(1000);
            task.stopNow();
        }
        Thread.sleep(3000);
        for (ControllableMsterExecutor<String> task : tasks) {
            Thread.sleep(1000);
            task.start();
        }
        Iterator<ControllableMsterExecutor<String>> iterator = tasks.iterator();
        while (iterator.hasNext()) {
            ControllableMsterExecutor<String> task = iterator.next();
            Thread.sleep(1000);
            task.shutdown();
            iterator.remove();
            System.gc();
        }
        Thread.sleep(1000 * 60);
    }

    public static ControllableMsterExecutor<String> createTask(String name, ExecutorProperties properties) {
        return new ControllableMsterExecutor.Builder<String>()
                .name(name)
                .executorProperties(properties)
                .masterPuller(() -> {
                    try {
                        Thread.sleep(new Random().nextInt(20));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    ArrayList<String> arr = new ArrayList<>();
                    arr.add("hello");
                    return arr;
                })
                .workerConsumer((data) -> {
                    try {
                        Thread.sleep(new Random().nextInt(50));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();
    }
}
