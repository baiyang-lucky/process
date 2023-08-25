package com.tbox.process.support;

import com.tbox.process.MasterPuller;
import com.tbox.process.Worker;
import com.tbox.process.WorkerCreateClose;
import com.tbox.process.exception.ExecutorException;
import com.tbox.process.support.workercc.BusyRateWorkerCreateClose;
import com.tbox.process.type.Event;
import com.tbox.process.type.LimitVelocityStrategy;
import com.tbox.process.type.State;
import com.tbox.process.type.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
public class MasterExecutor<T> extends AbstractMasterExecutor<T> {
    private static final Logger logger = LoggerFactory.getLogger(MasterExecutor.class);

    /**
     * workerId池
     */
    private final WorkerIdPool workerIdPool = new WorkerIdPool();
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

    /**
     * Woker创建与关闭判决
     */
    private WorkerCreateClose workerCreateClose;

    /**
     * 用于存放临时Worker
     */
    private final List<SimpleWorker<T>> tempWorkers = Collections.synchronizedList(new ArrayList<>());

    public MasterExecutor(String name, ExecutorProperties processProperties, BlockingQueue<T> dataQueue, MasterPuller<T> masterPuller, Consumer<T> workerConsumer, WorkerCreateClose workerCreateClose) {
        super(name, processProperties, dataQueue, masterPuller, workerConsumer);
        this.semaphore = new Semaphore(0);
        this.fsm = new FSM(name);
        ReentrantLock lock = new ReentrantLock();
        startNotify = new Tuple2<>(lock, lock.newCondition());
        this.workerCreateClose = workerCreateClose;
        if (this.workerCreateClose == null) {
            this.workerCreateClose = new BusyRateWorkerCreateClose();
        }
        //注册当前执行器到全局
        GlobalExecutorRegistry.register(this, new ExecutorContext(name, this.dataQueue, workers, fsm, processProperties));
    }

    /**
     * 创建Worker
     */
    @Override
    protected Worker<T> doCreateWorker() {
        int workerRole = SimpleWorker.TEMPORARY_WORKER; //临时wroker
        if (this.workers.size() < processProperties.getCoreWorkerSize()) {
            workerRole = SimpleWorker.CORE_WOKER; //核心worker
        }
        int workerId = workerIdPool.getWorkerId(); //获取workerId
        String workerName = String.format("Worker-%s-%d", name, workerId); //生成workerName
        logger.info("Create Worker[{}].", workerName);
        SimpleWorker<T> worker = new SimpleWorker.Builder<T>()
                .name(workerName)
                .workerId(workerId)
                .fsm(fsm)
                .semaphore(semaphore)
                .startNotify(startNotify)
                .dataQueue(dataQueue)
                .wokerRole(workerRole)
                .workerConsumer(workerConsumer)
                .processProperties(processProperties)
                .shutdownCall(this::removeWorker) //Worker关闭回调，从wroker列表中移除当前worker
                .build();
        if (workerRole == SimpleWorker.TEMPORARY_WORKER) {
            tempWorkers.add(worker); //如果是临时，则加入到临时Worker列表
        }
        return worker;
    }

    /**
     * 移除Worker
     */
    @Override
    protected void removeWorker(Worker<?> worker) {
        super.removeWorker(worker);
        SimpleWorker<?> simpleWorker = (SimpleWorker<?>) worker;
        if (simpleWorker.curWorkerRole == SimpleWorker.TEMPORARY_WORKER) { //如果是临时worker则从临时worker列表移除
            tempWorkers.remove(simpleWorker); //如果是临时，则加入到临时Worker列表
        }
        workerIdPool.releaseWorkerId(simpleWorker.getWorkerId());//释放归还workerId，在后续新创建worker时可以复用
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
                executeOnce((restDatas) -> {
                    //full back 处理
//                    logger.debug("队列满，剩余未处理：" + restDatas.size());
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
//                    logger.debug("full back结束：" + restDatas);
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
        determineWorkerLife();
    }

    /**
     * 存放最近创建关闭时间
     */
    private long determineCreateCloseTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

    /**
     * 通过workerCreateClose来判决是否需要创建新worker还是关闭一个临时worker。
     */
    private void determineWorkerLife() {
        if (workerCreateClose == null) {
            return;
        }
        if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - determineCreateCloseTime < workerCreateClose.determineInterval()) { //时间间隔判断
            return;
        }
        int workerCount = workers.size();
        int accumulationCount = dataQueue.size();
        int queueLenght = processProperties.getQueueSize();
        int idleWorkerCount = semaphore.drainPermits();
        boolean create = false;
        boolean close = false;
        if (workerCount < processProperties.getMaxWorkerSize()) {
            create = workerCreateClose.determineCreateWorker(accumulationCount, queueLenght, workerCount, idleWorkerCount);
        }
        if (workerCount > processProperties.getCoreWorkerSize()) {
            close = workerCreateClose.determineCloseWorker(accumulationCount, queueLenght, workerCount, idleWorkerCount);
        }
        if (create != close) { //当不相等时才生效
            if (create) {
                createWorker().start(); // 创建worker并启动
            } else {
                Iterator<SimpleWorker<T>> iterator = tempWorkers.iterator();
                if (iterator.hasNext()) {
                    iterator.next().close(); //关闭临时worker
                }
            }
        }
        determineCreateCloseTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
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
            ArrayList<Worker<?>> copyWorkers = new ArrayList<>(workers);
            for (Worker<?> worker : copyWorkers) {
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


    public static class Builder<T> {
        private String name;
        private ExecutorProperties executorProperties;
        private MasterPuller<T> masterPuller;
        private Consumer<T> workerConsumer;
        private WorkerCreateClose workerCreateClose = new BusyRateWorkerCreateClose();

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

        public Builder<T> workerCreateClose(WorkerCreateClose workerCreateClose) {
            this.workerCreateClose = workerCreateClose;
            return this;
        }


        public MasterExecutor<T> build() {
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
            return new MasterExecutor<T>(name, executorProperties, dataQueue, masterPuller, workerConsumer, workerCreateClose);
        }

    }

}
