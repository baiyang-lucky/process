package com.tbox.process.support;


import com.tbox.process.Master;
import com.tbox.process.type.State;
import com.tbox.process.type.Tuple5;

import java.util.Comparator;

/**
 * 全局执行器注册代理，用于外部访问，隔离对GlobalExecutorRegistry的直接访问。
 * 该类提供了针对已注册的Master执行情况感知。
 *
 * @author 白杨
 * DateTime:2023/8/23 10:43
 */
public class GlobalExecutorRegistryAgent {

    public static long getInitCount() {
        return GlobalExecutorRegistry.registry.values().stream().
                filter((context) -> context.getFsm().state() == State.INIT)
                .count();
    }

    public static long getReadyCount() {
        return GlobalExecutorRegistry.registry.values().stream().
                filter((context) -> context.getFsm().state() == State.READY)
                .count();
    }

    public static long getRunningCount() {
        return GlobalExecutorRegistry.registry.values().stream().
                filter((context) -> context.getFsm().state() == State.RUNNING)
                .count();
    }

    public static long getStopCount() {
        return GlobalExecutorRegistry.registry.values().stream().
                filter((context) -> context.getFsm().state() == State.STOP)
                .count();
    }

    /**
     * 获取关闭的任务数量。
     * 由于任务shutdown之后，其Master正常会回收，所以该方法大部分情况下返回0。
     */
    public static long getShutdownCount() {
        return GlobalExecutorRegistry.registry.values().stream()
                .filter((context) -> context.getFsm().state() == State.SHUTDOWN)
                .count();
    }

    /**
     * 获取所有状态对应数量。
     *
     * @return Tuple5 5元组，按照顺序：初始数量、准备数量、运行数量、停止数量、关闭数量。
     */
    public static Tuple5<Long, Long, Long, Long, Long> getAllStateCount() {
        long initCount = 0;
        long readyCount = 0;
        long runningCount = 0;
        long stopCount = 0;
        long shutdownCount = 0;
        for (ExecutorContext value : GlobalExecutorRegistry.registry.values()) {
            switch (value.getFsm().state()) {
                case INIT:
                    initCount += 1;
                    break;
                case READY:
                    readyCount += 1;
                    break;
                case RUNNING:
                    runningCount += 1;
                    break;
                case STOP:
                    stopCount += 1;
                    break;
                case SHUTDOWN:
                    shutdownCount += 1;
                    break;
            }
        }
        return new Tuple5<>(initCount, readyCount, runningCount, stopCount, shutdownCount);
    }

    /**
     * 按照任务名来获取上下文信息
     */
    public static ExecutorContext findExcutorContext(String name) {
        return GlobalExecutorRegistry.registry.values().stream().filter((context) -> context.getName().equals(name)).findAny().orElse(null);
    }

    /**
     * 获取积压最小的执行器上下文
     */
    public static ExecutorContext findMinPressure() {
        return GlobalExecutorRegistry.registry.values().stream().
                filter(context -> context.getFsm().state() == State.RUNNING || context.getFsm().state() == State.STOP)
                .min(Comparator.comparingInt(o -> o.getQueue().size()))
                .orElse(null);
    }

    /**
     * 获取积压最多的执行器上下文
     */
    public static ExecutorContext findMaxPressure() {
        return GlobalExecutorRegistry.registry.values().stream()
                .filter(context -> context.getFsm().state() == State.RUNNING || context.getFsm().state() == State.STOP)
                .max(Comparator.comparingInt(o -> o.getQueue().size()))
                .orElse(null);
    }

    /**
     * 获取根据名称获取Master
     */
    public static Master<?> findMaster(String name) {
        return GlobalExecutorRegistry.registry.keySet().stream().filter((context) -> context.getName().equals(name)).findAny().orElse(null);
    }
}
