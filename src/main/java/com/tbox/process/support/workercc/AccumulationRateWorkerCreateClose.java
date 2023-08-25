package com.tbox.process.support.workercc;

import com.tbox.process.WorkerCreateClose;
import com.tbox.process.exception.ExecutorException;

/**
 * 根据堆积程度来创建或关闭worker
 */
public class AccumulationRateWorkerCreateClose implements WorkerCreateClose {

    /**
     * 间隔时间,单位毫秒
     */
    private long interval = 2000;

    /**
     * 创建使用的堆积率
     */
    private final double createRate;

    /**
     * 关闭使用的堆积率
     */
    private final double closeRate;

    /**
     * @param createRate 当堆积率达到createRate，则创建worker
     * @param closeRate  当堆积率降到closeRate，则关闭worker
     */
    public AccumulationRateWorkerCreateClose(double createRate, double closeRate) {
        if (createRate > 1 || closeRate > 1 || createRate < 0 || closeRate < 0) {
            throw new ExecutorException("createRate and closeRate must be between 0-1.");
        }
        this.createRate = createRate;
        this.closeRate = closeRate;
    }

    /**
     * @param createRate 当堆积率达到createRate，则创建worker
     * @param closeRate  当堆积率降到closeRate，则关闭worker
     * @param interval   判断间隔时间,单位毫秒
     */
    public AccumulationRateWorkerCreateClose(double createRate, double closeRate, long interval) {
        this(createRate, closeRate);
        this.interval = interval;
    }

    @Override
    public long determineInterval() {
        return interval;
    }

    @Override
    public boolean determineCreateWorker(long accumulationCount, long dataQueueLength, int workerCount, int idleWorkerCount) {
        return ((double) accumulationCount / dataQueueLength) >= createRate;
    }

    @Override
    public boolean determineCloseWorker(long accumulationCount, long dataQueueLength, int workerCount, int idleWorkerCount) {
        return ((double) accumulationCount / dataQueueLength) <= closeRate;
    }
}
