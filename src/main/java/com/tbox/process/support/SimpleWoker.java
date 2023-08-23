package com.tbox.process.support;

import com.tbox.process.Worker;
import com.tbox.process.type.EventState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Woker实现，内部根据FSM来实现状态的监控，并且接收一个Condition来获取启动通知。
 * workerConsumer为具体的外部消费实现。
 * 同时内部通过信号量来使外部Master感知当前总体的woker是否处于繁忙状态。
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public class SimpleWoker<T> extends Thread implements Worker<T> {
    private static final Logger logger = LoggerFactory.getLogger(SimpleWoker.class);

    /**
     * 数据队列
     */
    private final LinkedBlockingDeque<T> dataQueue;
    /**
     * 消费动作，实际处理方法
     */
    private final Consumer<T> workerConsumer;

    /**
     * 分配的workerId
     */
    private final int workerId;
    /**
     * 处理器属性
     */
    private final ExecutorProperties processProperties;
    /**
     * 多线程等待通知
     */
    private final ReentrantLock lock;
    private final Condition startNotify;
    /**
     * worker信号量
     */
    private final Semaphore semaphore;
    /**
     * 状态机
     */
    private volatile FSM fsm;

    public SimpleWoker(String name, int workerId, Semaphore semaphore, FSM fsm, ReentrantLock lock, Condition startNotify,
                       LinkedBlockingDeque<T> dataQueue, Consumer<T> workerConsumer, ExecutorProperties processProperties) {
        super(name);
        this.workerId = workerId;
        this.dataQueue = dataQueue;
        this.semaphore = semaphore;
        this.fsm = fsm;
        this.workerConsumer = workerConsumer;
        this.processProperties = processProperties;
        this.lock = lock;
        this.startNotify = startNotify;
    }

    /**
     * 实现不做无限时长阻塞，依靠阻塞超时来驱动状态流转。
     * 停止启动依靠condition来驱动。
     */
    @Override
    public void run() {
        while (true) {
            semaphore.release(); //释放信号量+1
            try {
                if (fsm.eventState() == EventState.RUN) { //运行
                    T data = dataQueue.poll(processProperties.getWorkerPullTimeout(), TimeUnit.MILLISECONDS);
                    if (data != null) {
                        logger.info("Woker[{}]:{}", getName(), dataQueue.size());
                        this.workerConsumer.accept(data); //执行实际消费动作
                        semaphore.release(); //释放信号量+1
                    }
                }

                if (fsm.eventState() == EventState.SHUTDOWN_NOW) { // 立马关闭：直接结束线程
                    logger.info("Woker[{}] take shutdown_now signal.", getName());
                    break;
                } else if (fsm.eventState() == EventState.SHUTDOWN) { // 关闭：消费完当前队列，然后结束线程
                    logger.info("Woker[{}] take shutdown signal.", getName());
                    fastConsume(); //快速消费，不阻塞，获取null直接结束
                    break;
                } else if (fsm.eventState() == EventState.STOP) { // 停止：消费完队列数据，然后等待Master启动信号
                    logger.info("Woker[{}] take stop signal.", getName());
                    fastConsume(); //快速消费，不阻塞，获取null直接结束
                    this.awaitStart(); //等待启动
                } else if (fsm.eventState() == EventState.STOP_NOW) { //立马停止：停止消费，直接等待Master启动信号
                    logger.info("Woker[{}] take stop_now signal.", getName());
                    this.awaitStart(); //等待启动
                }

            } catch (InterruptedException e) {
                logger.error("Woker[{}] Interrupted.", getName());
            }
        }
        logger.info("Woker[{}] closed.", getName());
    }

    public void fastConsume() {
        T data = null;
        while ((data = dataQueue.poll()) != null) {
//            logger.info("Woker[{}]:{}", getName(), dataQueue.size());
            this.workerConsumer.accept(data);
        }
    }

    public void awaitStart() throws InterruptedException {
        try {
            lock.lock();
            this.startNotify.await(3, TimeUnit.SECONDS);//等待启动通知
        } finally {
            lock.unlock();
        }
    }

    public int getWorkerId() {
        return workerId;
    }
}
