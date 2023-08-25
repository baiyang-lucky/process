package com.tbox.process.support.workercc;

import com.tbox.process.WorkerCreateClose;

/**
 * 如果没有空闲worker就创建worker。
 * 当生产速度快，则worker空闲少，生产速度慢，worker空闲率多
 */
public class BusyRateWorkerCreateClose implements WorkerCreateClose {
    private double closeIdleRate = 0.3;
    private long interval = 5000;

    public BusyRateWorkerCreateClose() {
    }

    /**
     * @param closeIdleRate 关闭worker空闲率；当worker空闲率达到该阈值，则关闭一个worker
     */
    public BusyRateWorkerCreateClose(double closeIdleRate) {
        this.closeIdleRate = closeIdleRate;
    }

    /**
     * @param closeIdleRate 关闭worker空闲率；当worker空闲率达到该阈值，则关闭一个worker
     * @param interval      判断间隔时间,单位毫秒
     */
    public BusyRateWorkerCreateClose(double closeIdleRate, long interval) {
        this(closeIdleRate);
        this.interval = interval;
    }

    @Override
    public long determineInterval() {
        return interval;
    }

    @Override
    public boolean determineCreateWorker(long overstockCount, long dataQueueLength, int workerCount, int idleWorkerCount) {
        return idleWorkerCount == 0;
    }

    @Override
    public boolean determineCloseWorker(long overstockCount, long dataQueueLength, int workerCount, int idleWorkerCount) {
        return (double) idleWorkerCount / workerCount > closeIdleRate;
    }
}
