package com.tbox.process.support;

import com.tbox.process.exception.ExecutorStateException;
import com.tbox.process.type.EventState;
import com.tbox.process.type.ProcessState;

/**
 * 执行器状态机
 * 维护事件与执行状态
 */
public class FSM {
    private String name;
    private EventState eventState = EventState.STOP;
    private ProcessState processState = ProcessState.INIT;

    public FSM(String name) {
        this.name = name;
    }

    public EventState eventState() {
        return eventState;
    }

    public ProcessState processState() {
        return processState;
    }

    public void ready(Action action) {
        if (processState == ProcessState.READY) {
            return;
        }
        if (processState != ProcessState.INIT) { //如果不是初始状态
            throw new ExecutorStateException(String.format("FSM[%s] not init state.", name));
        }
        action.handle();
        processState = ProcessState.READY; //设置为就绪状态
    }

    public void start(Action action) {
        if (processState == ProcessState.RUNNING) {//运行中
            return;
        }
        if (processState == ProcessState.SHUTDOWN) { //关闭状态
            throw new ExecutorStateException(String.format("FSM[%s] already shutdown.", name));
        }
        eventState = EventState.RUN; //执行状态设置为RUN (通知Woker)
        action.handle();
        processState = ProcessState.RUNNING; // 切换状态为运行中
    }

    public void stop(Action action) {
        if (processState == ProcessState.INIT) { //如果是初始状态
            throw new ExecutorStateException(String.format("FSM[%s] not init state.", name));
        }
        if (processState == ProcessState.SHUTDOWN) { //关闭状态
            throw new ExecutorStateException(String.format("FSM[%s] already shutdown.", name));
        }
        eventState = EventState.STOP;
        action.handle();
        processState = ProcessState.STOP; // 停止
    }

    public void stopNow(Action action) {
        if (processState == ProcessState.INIT) { //如果是初始状态
            throw new ExecutorStateException(String.format("FSM[%s] not init state.", name));
        }
        if (processState == ProcessState.SHUTDOWN) { //关闭状态
            throw new ExecutorStateException(String.format("FSM[%s] already shutdown.", name));
        }
        eventState = EventState.STOP_NOW;// 执行状态设置为STOP_NOW (通知Woker)
        action.handle();
        processState = ProcessState.STOP; // 停止
    }

    public void shutdown(Action action) {
        eventState = EventState.SHUTDOWN;// 执行状态设置为SHUTDOWN (通知Woker)
        action.handle();
        processState = ProcessState.SHUTDOWN;//关闭
    }

    public void shutdownNow(Action action) {
        eventState = EventState.SHUTDOWN_NOW;// 执行状态设置为SHUTDOWN_NOW (通知Woker)
        action.handle();
        processState = ProcessState.SHUTDOWN;//关闭
    }

    @FunctionalInterface
    public interface Action {
        void handle();
    }
}
