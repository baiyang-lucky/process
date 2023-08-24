package com.tbox.process.support;

import com.tbox.process.MasterPuller;
import com.tbox.process.Worker;
import com.tbox.process.exception.ExecutorException;
import com.tbox.process.type.Event;
import com.tbox.process.type.LimitVelocityStrategy;
import com.tbox.process.type.State;
import com.tbox.process.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 可控制的MasterExecutor。提供了启动、停止、关闭等控制操作。内部维护了FSM状态机，用来实现状态控制流转。根据当前Worker繁忙程度来动态创建Worker。
 * Master拉取数据为独立的线程。
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public class MsterExecutor<T> extends AbstrctMasterExecutor<T> {
    private static final Logger logger = LoggerFactory.getLogger(MsterExecutor.class);
    private volatile int workerIdFlag = 0;
    /**
     * worker信号量
     */
    protected final Semaphore semaphore;
    /**
     * 当前状态
     */
    protected final State state = State.INIT;
    /**
     * 状态机
     */
    protected final FSM fsm;
    /**
     * Master线程
     */
    protected Thread masterTh;
    /**
     * 多线程等待通知
     */
    protected final Tuple2<Lock, Condition> startNotify;

    public MsterExecutor(String name, ExecutorProperties processProperties, BlockingQueue<T> dataQueue, MasterPuller<T> masterPuller, Consumer<T> workerConsumer) {
        super(name, processProperties, dataQueue, masterPuller, workerConsumer);
        this.semaphore = new Semaphore(0);
        this.fsm = new FSM(name);
        ReentrantLock lock = new ReentrantLock();
        startNotify = new Tuple2<>(lock, lock.newCondition());
        //注册当前执行器到全局
        GlobalExecutorRegistry.register(this, new ExecutorContext(name, this.dataQueue, workers, fsm, processProperties));
    }

    /**
     * 创建Worker
     */
    @Override
    protected synchronized Worker<T> doCreateWorker() {
        int wokerRole = SimpleWorker.TEMPORARY_WORKER; //临时woker
        if (this.workers.size() < processProperties.getCoreWorkerSize()) {
            wokerRole = SimpleWorker.CORE_WOKER; //核心woker
        }
        int workerId = getWorkerId(); //获取workerId
        String workerName = String.format("Worker-%s-%d", name, workerId); //生成workerName
        logger.info("Create Worker[{}] .", workerName);
        return new SimpleWorker.Builder<T>()
                .name(workerName)
                .workerId(workerId)
                .fsm(fsm)
                .semaphore(semaphore)
                .startNotify(startNotify)
                .dataQueue(dataQueue)
                .wokerRole(wokerRole)
                .workerConsumer(workerConsumer)
                .processProperties(processProperties)
                .shutdownCall(this::removeWorker) //Woker关闭回调，从woker列表中移除当前worker
                .build();
    }

    /**
     * 移除Worker
     */
    @Override
    protected void removeWorker(Worker<?> worker) {
        SimpleWorker<?> simpleWorker = (SimpleWorker<?>) worker;
        releaseWorkerId(simpleWorker.getWorkerId());//释放归还workerId，在后续新创建worker时可以复用
        super.removeWorker(worker);
    }

    /**
     * 启动
     */
    @Override
    public void start() {
        synchronized (this) {
            if (this.fsm.state() == State.INIT) { // 如果是初始状态
                this.fsm.ready(() -> { //执行就绪
                    for (int i = 0; i < processProperties.getCoreWorkerSize(); i++) {
                        createWorker().start();
                    }
                });
            }
            this.fsm.start(() -> { //启动
                if (masterTh == null) { //如果Master线程为空，则表明第一次启动，需要创建Master线程
                    masterTh = new Thread(this::masterExecute, String.format("Master-%s", name));
                    masterTh.setDaemon(true);
                    masterTh.start(); //启动主线程
                }
                try {
                    startNotify.t1.lock();
                    startNotify.t2.signalAll(); //通知所有Worker继续
                } finally {
                    startNotify.t1.unlock();
                }
            });
        }
    }

    /**
     * Master线程执行体
     */
    private void masterExecute() {
        while (true) {
            if (fsm.event() == Event.RUN) {
                executeOnece((restDatas) -> {
                    //full back 处理
                    logger.debug("队列满，剩余未处理：" + restDatas.size());
                    for (T restData : restDatas) {
                        if (state == State.SHUTDOWN) {
                            logger.info("Take shutdown signal，stop full back process.");
                            break;
                        }
                        try {
                            while (!this.dataQueue.offer(restData, processProperties.getFullBackOfferTimeout(), TimeUnit.MILLISECONDS)) {
                            }
                        } catch (InterruptedException e) {
                            logger.error("Master[{}] fullback trigger InterruptedException.", name);
                            this.shutdown(); // 关闭
                            throw new ExecutorException(String.format("Master[%s] fullback trigger InterruptedException", name));
                        }
                    }
                    logger.debug("full back结束：" + restDatas);
                });
            } else if (fsm.event() == Event.SHUTDOWN_NOW || fsm.event() == Event.SHUTDOWN) { //关闭
                logger.info("Master[{}] take shutdown signal.", name);
                break;
            } else if (fsm.event() == Event.STOP || fsm.event() == Event.STOP_NOW) {//停止
                logger.info("Master[{}] take stop signal.", name);
                try {
                    startNotify.t1.lock();
                    startNotify.t2.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("Master[{}] InterruptedException.", name);
                    this.shutdown(); // 关闭
                    throw new ExecutorException(String.format("Master[%s] fullback trigger InterruptedException", name));
                } finally {
                    startNotify.t1.unlock();
                }
            }
        }
        logger.info("Master[{}] closed.", name);
    }

    /**
     * 检查当前Worker是否有空闲，如果没有空闲并且Worker数量没有达到最大数，则创建Worker
     */
    @Override
    protected void putQueueBefore(T data) {
        // 如果核心worker处于繁忙，并且当期worker数小于最大数,则创建新worker
        if (!semaphore.tryAcquire() && workers.size() < processProperties.getMaxWorkerSize()) {
            createWorker().start(); // 创建worker并启动
        }
    }

    /**
     * 停止，等待worker完成当前任务。
     * 可恢复运行。
     */
    @Override
    public synchronized void stop() {
        this.fsm.stop(() -> {
        });
    }

    /**
     * 立马停止，队列中有数据也不会继续消费。
     * 可恢复运行。
     */
    @Override
    public synchronized void stopNow() {
        this.fsm.stopNow(() -> {
        });
    }

    /**
     * 关闭，等待Worker停止，内存回收。
     */
    @Override
    public synchronized void shutdown() {
        this.fsm.shutdown(this::waitAllThreadShutdown);
    }

    /**
     * 立即关闭，所有Worker停止，内存回收。
     */
    @Override
    public synchronized void shutdownNow() {
        this.fsm.shutdownNow(() -> {
            dataQueue.clear();
            waitAllThreadShutdown();
        });
    }

    /**
     * 等待所有线程消亡
     */
    private void waitAllThreadShutdown() {
        try {
            // 等待Master结束
            if (masterTh != null) {
                if (masterTh.isAlive()) {
                    masterTh.join();
                }
                masterTh = null;
            }
            // 等待Worker结束
            for (Worker<?> worker : workers) {
                SimpleWorker<?> simpleWorker = (SimpleWorker<?>) worker;
                if (simpleWorker.isAlive()) {
                    simpleWorker.join();
                }
            }
        } catch (InterruptedException e) {
        }
        workers.clear();
    }

    /**
     * 获取当前状态
     */
    public State getState() {
        return state;
    }

    /**
     * 获取workerId
     */
    protected synchronized int getWorkerId() {
        int i = 0;
        while (((workerIdFlag >> i) & 1) != 0) {
            i++;
        }
        workerIdFlag |= 1 << i;
        return i + 1;
    }

    /**
     * 释放归还workerId
     */
    private synchronized void releaseWorkerId(int workerId) {
        workerIdFlag &= ~(1 << (workerId - 1));
    }


    public static class Builder<T> {
        private String name;
        private ExecutorProperties executorProperties;
        private MasterPuller<T> masterPuller;
        private Consumer<T> workerConsumer;

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> executorProperties(ExecutorProperties executorProperties) {
            this.executorProperties = executorProperties;
            return this;
        }

        public Builder<T> masterPuller(MasterPuller<T> masterPuller) {
            this.masterPuller = masterPuller;
            return this;
        }

        public Builder<T> workerConsumer(Consumer<T> workerConsumer) {
            this.workerConsumer = workerConsumer;
            return this;
        }


        public MsterExecutor<T> build() {
            if (executorProperties == null) {
                executorProperties = new ExecutorProperties();
            }
            if (masterPuller == null) {
                throw new ExecutorException("masterPuller is require.");
            }
            if (workerConsumer == null) {
                throw new ExecutorException("workerConsumer is require.");
            }
            BlockingQueue<T> dataQueue;
            if (executorProperties.getLimitVelocityStrategy() == LimitVelocityStrategy.BALANCE) {
                //速度平衡使用同步队列
                dataQueue = new SynchronousQueue<>();
            } else {
                //不限速则意味着允许出现数据堆积，使用LinkedBlockingQueue
                dataQueue = new LinkedBlockingQueue<>(executorProperties.getQueueSize());
            }
            return new MsterExecutor<T>(name, executorProperties, dataQueue, masterPuller, workerConsumer);
        }

    }

}
