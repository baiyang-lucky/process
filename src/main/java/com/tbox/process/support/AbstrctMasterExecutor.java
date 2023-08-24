package com.tbox.process.support;

import com.tbox.process.Master;
import com.tbox.process.MasterPuller;
import com.tbox.process.Worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * Master处理基础抽象
 * 维护了一个dataQueue用于Master与Worker之间的数据传输，以及属性配置。
 * 维护了Worker的创建与worker队列，具体Worker的创建由doCreateWorker()方法实现。
 * executeOnece()方法执行最基本的拉取数据放入队列的操作，已经队列满时的fullBack。
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public abstract class AbstrctMasterExecutor<T> implements Master<T> {

    /**
     * 任务名称
     */
    protected String name;
    /**
     * 过程配置
     */
    protected final ExecutorProperties processProperties;
    /**
     * Master与Worker之间数据传输队列
     */
    protected final BlockingQueue<T> dataQueue;

    /**
     * 当前Master下Worker集合
     */
    protected List<Worker<?>> workers;

    /**
     * Master拉取器，用于从外部获取数据，由外部实现传入。
     */
    protected MasterPuller<T> masterPuller;

    /**
     * Worker消费者
     */
    protected Consumer<T> workerConsumer;

    public AbstrctMasterExecutor(String name, ExecutorProperties processProperties, BlockingQueue<T> dataQueue, MasterPuller<T> masterPuller, Consumer<T> workerConsumer) {
        this.name = name;
        this.processProperties = processProperties;
        this.dataQueue = dataQueue;
        this.workers = Collections.synchronizedList(new ArrayList<>(processProperties.getMaxWorkerSize()));
        this.masterPuller = masterPuller;
        this.workerConsumer = workerConsumer;
    }


    abstract protected Worker<T> doCreateWorker();

    /**
     * 创建Worker并放入worker列表
     *
     * @return 新创建的worker
     */
    @Override
    public Worker<T> createWorker() {
        Worker<T> worker = doCreateWorker();
        workers.add(worker);
        return worker;
    }

    protected void removeWorker(Worker<?> worker) {
        workers.remove(worker);
    }

    /**
     * 数据放入队列之前
     */
    protected void putQueueBefore(T data) {

    }


    /**
     * Master执行一次拉取数据并放入数据队列。
     * 如果数据队列已经满了，则调用fullBack对剩余未处理的数据列表进行处理。
     *
     * @param fullBack 当dataQueue满了无法放入数据时，调用该方法
     * @return 队列放入失败时返回false，所有数据成功放入队列返回true
     */
    protected boolean executeOnece(Consumer<List<T>> fullBack) {
        List<T> dataList = this.masterPuller.pull();//拉取数据
        for (int i = 0; i < dataList.size(); i++) {
            T data = dataList.get(i);
            putQueueBefore(data);//放入队列之前子类实现动作
            if (!this.dataQueue.offer(data)) { // 如果队列已满，数据放入失败
                if (fullBack != null) {
                    fullBack.accept(dataList.subList(i, dataList.size())); //调用fullBack传入剩余未放入成功的数据
                }
                return false;
            }
        }
        return true;
    }

    @Override
    public String getName() {
        return this.name;
    }

}
