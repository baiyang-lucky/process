package com.tbox.process;

/**
 * worker的创建时机与关闭时机实现。
 * 该接口将用于当执行任务上下文相关信息达到某种程度时，来决定是否需要创建worker或关闭worker。
 * 创建与关闭两个方法返回true和false，当两个方法同时返回true时，不会做任何操作，所以应该确保两个方法永远不要同时返回true，否则检查判定规则是否正确。
 */
public interface WorkerCreateClose {

    /**
     * 判断是否需要创建一个worker
     *
     * @param overstockCount  数据积压数量
     * @param dataQueueLength 数据队列长度
     * @param workerCount     当前woker数量
     * @param idleWorkerCount 当前空闲Woker数量
     * @return true-需要创建worker ； false-不需要创建worker；
     */
    boolean determinCreateWorker(long overstockCount, long dataQueueLength, int workerCount, int idleWorkerCount);

    /**
     * 判决是否需要关闭一个worker
     *
     * @param overstockCount  数据积压数量
     * @param dataQueueLength 数据队列长度
     * @param workerCount     当前woker数量
     * @param idleWorkerCount 当前空闲Woker数量
     * @return true-需要创建worker ； false-不需要创建worker；
     */
    boolean determineCloseWorker(long overstockCount, long dataQueueLength, int workerCount, int idleWorkerCount);
}
