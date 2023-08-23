package com.tbox.process.support;

import com.tbox.process.Worker;

import java.util.List;
import java.util.Queue;

/**
 * 执行器上下文
 */
public class ExecutorContext {
    /**
     * 任务名
     */
    private String name;
    /**
     * 数据队列
     */
    private Queue<?> queue;
    /**
     * Worker集合
     */
    private List<Worker<?>> workers;
    /**
     * 当前Executor状态机
     */
    private FSM fsm;

    private ExecutorProperties executorProperties;

    public ExecutorContext(String name, Queue<?> queue, List<Worker<?>> workers, FSM fsm, ExecutorProperties properties) {
        this.name = name;
        this.queue = queue;
        this.workers = workers;
        this.fsm = fsm;
        this.executorProperties = properties;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Queue<?> getQueue() {
        return queue;
    }

    public void setQueue(Queue<?> queue) {
        this.queue = queue;
    }

    public List<Worker<?>> getWorkers() {
        return workers;
    }

    public void setWorkers(List<Worker<?>> workers) {
        this.workers = workers;
    }

    public FSM getFsm() {
        return fsm;
    }

    public void setFsm(FSM fsm) {
        this.fsm = fsm;
    }

    public ExecutorProperties getExecutorProperties() {
        return executorProperties;
    }

    public void setExecutorProperties(ExecutorProperties executorProperties) {
        this.executorProperties = executorProperties;
    }
}
