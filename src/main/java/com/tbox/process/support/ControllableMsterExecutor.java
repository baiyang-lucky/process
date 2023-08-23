package com.tbox.process.support;

import com.tbox.process.MasterPuller;
import com.tbox.process.Worker;
import com.tbox.process.exception.ExecutorException;
import com.tbox.process.type.EventState;
import com.tbox.process.type.ProcessState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 可控制的MasterExecutor。提供了启动、停止、关闭等控制操作。内部维护了FSM状态机，用来实现状态控制流转。根据当前Woker繁忙程度来动态创建Woker。
 * Master拉取数据为独立的线程。
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public class ControllableMsterExecutor<T> extends AbstrctMasterExecutor<T> {
    private static final Logger logger = LoggerFactory.getLogger(ControllableMsterExecutor.class);
    private volatile int workerIdFlag = 0;
    /**
     * worker信号量
     */
    private final Semaphore semaphore;
    /**
     * 当前状态
     */
    private volatile ProcessState processState = ProcessState.INIT;
    /**
     * 状态机
     */
    private volatile FSM fsm;
    /**
     * 多线程等待通知
     */
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition startNotify = lock.newCondition();

    public ControllableMsterExecutor(String name, ExecutorProperties processProperties, MasterPuller<T> masterPuller, Consumer<T> workerConsumer) {
        super(name, processProperties, masterPuller, workerConsumer);
        this.semaphore = new Semaphore(0);
        this.fsm = new FSM(name);
    }

    @Override
    protected Worker<T> doCreateWorker() {
        int wokerId = getWokerId();
        String workerName = String.format("Woker-%s-%d", name, wokerId);
        logger.info("Create woker[{}] .", workerName);
        return new SimpleWoker<>(workerName, wokerId,
                semaphore, fsm, lock, startNotify, this.dataQueue, this.workerConsumer, this.processProperties);
    }

    @Override
    protected void removeWorker(Worker<?> worker) {
        SimpleWoker simpleWoker = (SimpleWoker) worker;
        releaseWorkerId(simpleWoker.getWorkerId());//释放workerId，在后续新创建worker时可以复用
        super.removeWorker(worker);
    }

    @Override
    public void start() {
        synchronized (this) {
            if (this.fsm.processState() == ProcessState.INIT) { // 如果是初始状态
                this.fsm.ready(() -> { //执行就绪
                    for (int i = 0; i < processProperties.getCoreWorkerSize(); i++) {
                        createWorker().start();
                    }
                });
            }
            this.fsm.start(() -> { //启动
                try {
                    lock.lock();
                    startNotify.signalAll(); //通知所有woker继续
                } finally {
                    lock.unlock();
                }
            });
        }
        new Thread(this::execute).start(); //启动主线程
    }

    private void execute() {
        while (true) {
            System.out.println("Master:" + fsm.eventState().name() + ":" + dataQueue.size());
            if (fsm.eventState() == EventState.RUN) {
                executeOnece((restDatas) -> {
                    //full back 处理
                    logger.debug("队列满，剩余未处理：" + restDatas.size());
                    for (T restData : restDatas) {
                        if (processState == ProcessState.SHUTDOWN) {
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
            } else if (fsm.eventState() == EventState.SHUTDOWN_NOW || fsm.eventState() == EventState.SHUTDOWN) { //关闭
                logger.info("Master[{}] take shutdown signal.", name);
                break;
            } else if (fsm.eventState() == EventState.STOP || fsm.eventState() == EventState.STOP_NOW) {//停止
                logger.info("Master[{}] take stop signal.", name);
                try {
                    lock.lock();
                    startNotify.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("Master[{}] InterruptedException.", name);
                    this.shutdown(); // 关闭
                    throw new ExecutorException(String.format("Master[%s] fullback trigger InterruptedException", name));
                } finally {
                    lock.unlock();
                }
            }
        }
        logger.info("Master[{}] closed.", name);
    }

    /**
     * 检查当前Woker是否有空闲，如果没有空闲并且woker数量没有达到最大数，则创建woker
     */
    @Override
    protected void putQueueBefore(T data) {
        // 如果核心worker处于繁忙，并且当期worker数小于最大数,则创建新worker
        if (!semaphore.tryAcquire() && workers.size() < processProperties.getMaxWorkderSize()) {
            createWorker().start(); // 创建worker并启动
        }
    }

    @Override
    public synchronized void stop() {
        this.fsm.stop(() -> {
        });
    }

    @Override
    public synchronized void stopNow() {
        this.fsm.stopNow(() -> {
        });
    }

    @Override
    public synchronized void shutdown() {
        this.fsm.shutdown(() -> {
            for (Worker<T> worker : workers) {
                SimpleWoker simpleWoker = (SimpleWoker) worker;
                if (simpleWoker.isAlive()) {
                    try {
                        simpleWoker.join();
                    } catch (InterruptedException e) {
                    }
                }
            }
            workers.clear();
        });
    }

    @Override
    public synchronized void shutdownNow() {
        this.fsm.shutdownNow(() -> {
            dataQueue.clear();
            for (Worker<T> worker : workers) {
                SimpleWoker simpleWoker = (SimpleWoker) worker;
                if (simpleWoker.isAlive()) {
                    try {
                        simpleWoker.join();
                    } catch (InterruptedException e) {
                    }
                }
            }
            workers.clear();
        });
    }

    public ProcessState getState() {
        return processState;
    }

    private synchronized int getWokerId() {
        int i = 0;
        for (; ((workerIdFlag >> i) & 1) != 0; ) {
            i++;
        }
        workerIdFlag |= 1 << i;
        return i + 1;
    }

    private synchronized void releaseWorkerId(int workerId) {
        workerIdFlag &= ~(1 << (workerId - 1));
    }


    public static class Builder<T> {
        private String name;
        private ExecutorProperties processProperties;
        private MasterPuller<T> masterPuller;
        private Consumer<T> workerConsumer;

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> processProperties(ExecutorProperties processProperties) {
            this.processProperties = processProperties;
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


        public ControllableMsterExecutor<T> build() {
            if (processProperties == null) {
                processProperties = new ExecutorProperties();
            }
            if (masterPuller == null) {
                throw new ExecutorException("masterPuller is require.");
            }
            if (workerConsumer == null) {
                throw new ExecutorException("workerConsumer is require.");
            }
            return new ControllableMsterExecutor<T>(name, processProperties, masterPuller, workerConsumer);
        }

    }

}
