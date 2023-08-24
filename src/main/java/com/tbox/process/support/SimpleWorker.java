package com.tbox.process.support;

import com.tbox.process.Worker;
import com.tbox.process.exception.ExecutorException;
import com.tbox.process.type.Event;
import com.tbox.process.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * Worker实现，内部根据FSM来实现状态的监控，并且接收一个Condition来获取启动通知。
 * workerConsumer为具体的外部消费实现。
 * 同时内部通过信号量来使外部Master感知当前总体的Worker是否处于繁忙状态。
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public class SimpleWorker<T> extends Thread implements Worker<T> {
    private static final Logger logger = LoggerFactory.getLogger(SimpleWorker.class);
    public static final int CORE_WOKER = 1; //核心woker 会一直存活
    public static final int TEMPORARY_WORKER = 2; //临时worker 会首先存活时间
    public final int curWokerRole; //当前woker角色：核心(CORE_WOKER)/临时(TEMPORARY_WORKER)
    /**
     * 数据队列
     */
    private final BlockingQueue<T> dataQueue;
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
    private final Tuple2<Lock, Condition> startNotify;
    /**
     * worker信号量
     */
    private final Semaphore semaphore;
    /**
     * 状态机
     */
    private final FSM fsm;

    /**
     * 线程关闭时回调
     */
    private final Consumer<Worker<T>> shutdownCall;


    public SimpleWorker(String name, int workerId, Semaphore semaphore, FSM fsm, Tuple2<Lock, Condition> startNotify,
                        BlockingQueue<T> dataQueue, Consumer<T> workerConsumer, ExecutorProperties processProperties,
                        Consumer<Worker<T>> shutdownCall, int wokerRole) {
        super(name);
        this.workerId = workerId;
        this.dataQueue = dataQueue;
        this.semaphore = semaphore;
        this.fsm = fsm;
        this.workerConsumer = workerConsumer;
        this.processProperties = processProperties;
        this.startNotify = startNotify;
        this.shutdownCall = shutdownCall;
        this.curWokerRole = wokerRole;
    }

    /**
     * 实现不做无限时长阻塞，依靠阻塞超时来驱动状态流转。
     * 停止启动依靠condition来驱动。
     */
    @Override
    public void run() {
        semaphore.release(); //释放信号量+1
        long workerPullTimeout = processProperties.getWorkerPullTimeout(); // 拉取数据超时时间
        long alivetime = this.curWokerRole == SimpleWorker.TEMPORARY_WORKER ? processProperties.getWorkerAlivetime() : -1;//存活时间
        long restAliveTime = Long.MAX_VALUE;//线程剩余存活时间，只有当Worker非核心时，会计算剩余存活时间
        long beginTime = System.nanoTime(); //起始时间,单位纳秒
        while (true) {
            try {
                if (alivetime > 0) {//只有存活时间>0才做存活判断
                    long curNanoTime = System.nanoTime();
                    long duration = TimeUnit.NANOSECONDS.toMillis(curNanoTime - beginTime);
                    restAliveTime = alivetime - duration; //最大存活时间-以经过时间=剩余存活时间
                    if (duration >= alivetime) { //如果存活时间大于设置的存活时间，则结束当前线程
                        break;
                    }
                }
                if (fsm.event() == Event.RUN) { //运行
                    //剩余存活时间>拉取超时时间，则使用拉取超时时间等待，否则使用剩余存活时间等待。更精准的时间控制
                    T data = dataQueue.poll(Math.min(restAliveTime, workerPullTimeout), TimeUnit.MILLISECONDS);
                    if (data != null) {
                        beginTime = System.nanoTime();//获取到了数据，则重置存活开始时间
                        semaphore.tryAcquire();//扣减信号量-1
                        this.workerConsumer.accept(data); //执行实际消费动作
                        semaphore.release(); //释放信号量+1
                    }
                }

                if (fsm.event() == Event.SHUTDOWN_NOW) { // 立马关闭：直接结束线程
                    logger.info("Worker[{}] take shutdown_now signal.", getName());
                    break;
                } else if (fsm.event() == Event.SHUTDOWN) { // 关闭：消费完当前队列，然后结束线程
                    logger.info("Worker[{}] take shutdown signal.", getName());
                    fastConsume(); //快速消费，不阻塞，获取null直接结束
                    break;
                } else if (fsm.event() == Event.STOP) { // 停止：消费完队列数据，然后等待Master启动信号
                    logger.info("Worker[{}] take stop signal.", getName());
                    fastConsume(); //快速消费，不阻塞，获取null直接结束
                    this.awaitStart(restAliveTime); //等待启动
                } else if (fsm.event() == Event.STOP_NOW) { //立马停止：停止消费，直接等待Master启动信号
                    logger.info("Worker[{}] take stop_now signal.", getName());
                    this.awaitStart(restAliveTime); //等待启动
                }
            } catch (InterruptedException e) {
                logger.error("Worker[{}] Interrupted.", getName());
                break;
            }
        }
        semaphore.tryAcquire();//扣减信号量-1
        shutdownCall.accept(this); //线程关闭回调
        logger.info("Worker[{}] closed.", getName());
    }

    public void fastConsume() {
        T data;
        while ((data = dataQueue.poll()) != null) {
            this.workerConsumer.accept(data);
        }
    }

    public void awaitStart(long restAliveTime) throws InterruptedException {
        try {
            this.startNotify.t1.lock();
            //剩余存活时间与3秒取最小等待
            this.startNotify.t2.await(Math.min(restAliveTime, 3000), TimeUnit.MILLISECONDS);//等待启动通知
        } finally {
            this.startNotify.t1.unlock();
        }
    }

    public int getWorkerId() {
        return workerId;
    }

    public static class Builder<T> {
        protected String name;
        protected BlockingQueue<T> dataQueue;
        protected Consumer<T> workerConsumer;
        protected int workerId;
        protected ExecutorProperties processProperties;
        protected Tuple2<Lock, Condition> startNotify;
        protected Semaphore semaphore;
        protected FSM fsm;
        protected int wokerRole = SimpleWorker.TEMPORARY_WORKER; //默认临时woker
        protected Consumer<Worker<T>> shutdownCall = (d) -> {
        };

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> dataQueue(BlockingQueue<T> dataQueue) {
            this.dataQueue = dataQueue;
            return this;
        }

        public Builder<T> workerConsumer(Consumer<T> workerConsumer) {
            this.workerConsumer = workerConsumer;
            return this;
        }

        public Builder<T> workerId(int workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder<T> processProperties(ExecutorProperties processProperties) {
            this.processProperties = processProperties;
            return this;
        }

        public Builder<T> startNotify(Tuple2<Lock, Condition> startNotify) {
            this.startNotify = startNotify;
            return this;
        }

        public Builder<T> semaphore(Semaphore semaphore) {
            this.semaphore = semaphore;
            return this;
        }

        public Builder<T> shutdownCall(Consumer<Worker<T>> shutdownCall) {
            this.shutdownCall = shutdownCall;
            return this;
        }

        public Builder<T> wokerRole(int wokerRole) {
            if (wokerRole != SimpleWorker.TEMPORARY_WORKER && wokerRole != SimpleWorker.CORE_WOKER) {
                throw new ExecutorException("wokerRole Error.");
            }
            this.wokerRole = wokerRole;
            return this;
        }


        public Builder<T> fsm(FSM fsm) {
            this.fsm = fsm;
            return this;
        }

        public SimpleWorker<T> build() {
            SimpleWorker<T> worker = new SimpleWorker<>(this.name, this.workerId, this.semaphore,
                    this.fsm, this.startNotify, this.dataQueue, this.workerConsumer, this.processProperties,
                    this.shutdownCall, this.wokerRole);
            worker.setDaemon(true);
            return worker;
        }

    }
}
