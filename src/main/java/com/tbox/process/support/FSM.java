package com.tbox.process.support;

import com.tbox.process.exception.ExecutorStateException;
import com.tbox.process.type.Event;
import com.tbox.process.type.State;

/**
 * 执行器状态机
 * 维护事件与执行状态
 */
public class FSM {
    private final String name;
    private volatile Event event = Event.STOP;
    private volatile State state = State.INIT;

    public FSM(String name) {
        this.name = name;
    }

    public Event eventState() {
        return event;
    }

    public State state() {
        return state;
    }

    public void ready(Action action) {
        if (state == State.READY) {
            return;
        }
        if (state != State.INIT) { //如果不是初始状态
            throw new ExecutorStateException(String.format("FSM[%s] not init state.", name));
        }
        action.handle();
        state = State.READY; //设置为就绪状态
    }

    public void start(Action action) {
        if (state == State.RUNNING) {//运行中
            return;
        }
        if (state == State.SHUTDOWN) { //关闭状态
            throw new ExecutorStateException(String.format("FSM[%s] already shutdown.", name));
        }
        event = Event.RUN; //执行状态设置为RUN (通知Woker)
        action.handle();
        state = State.RUNNING; // 切换状态为运行中
    }

    public void stop(Action action) {
        if (state == State.INIT) { //如果是初始状态
            throw new ExecutorStateException(String.format("FSM[%s] not init state.", name));
        }
        if (state == State.SHUTDOWN) { //关闭状态
            throw new ExecutorStateException(String.format("FSM[%s] already shutdown.", name));
        }
        event = Event.STOP;
        action.handle();
        state = State.STOP; // 停止
    }

    public void stopNow(Action action) {
        if (state == State.INIT) { //如果是初始状态
            throw new ExecutorStateException(String.format("FSM[%s] not init state.", name));
        }
        if (state == State.SHUTDOWN) { //关闭状态
            throw new ExecutorStateException(String.format("FSM[%s] already shutdown.", name));
        }
        event = Event.STOP_NOW;// 执行状态设置为STOP_NOW (通知Woker)
        action.handle();
        state = State.STOP; // 停止
    }

    public void shutdown(Action action) {
        event = Event.SHUTDOWN;// 执行状态设置为SHUTDOWN (通知Woker)
        action.handle();
        state = State.SHUTDOWN;//关闭
    }

    public void shutdownNow(Action action) {
        event = Event.SHUTDOWN_NOW;// 执行状态设置为SHUTDOWN_NOW (通知Woker)
        action.handle();
        state = State.SHUTDOWN;//关闭
    }

    @FunctionalInterface
    public interface Action {
        void handle();
    }
}
