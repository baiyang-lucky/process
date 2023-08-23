package com.tbox.process;

public interface Master<T> {
    /**
     * 创建Worker对象
     */
    Worker<T> createWorker();

    /**
     * 启动
     */
    void start();

    /**
     * 停止，等待worker完成当前任务。
     * 可恢复运行。
     */
    void stop();

    /**
     * 立马停止，队列中有数据也不会继续消费。
     * 可恢复运行。
     */
    void stopNow();

    /**
     * 关闭，等待Worker停止，内存回收。
     */
    void shutdown();

    /**
     * 立即关闭，所有Worker停止，内存回收。
     */
    void shutdownNow();

}
