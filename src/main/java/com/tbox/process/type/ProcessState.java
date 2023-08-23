package com.tbox.process.type;

/**
 * 状态
 */
public enum ProcessState {
    /**
     * 初始，还未达到运行条件，运行需初始化。
     */
    INIT,
    /**
     * 就绪，所有数据与线程都已初始化好，等待运行。
     */
    READY,
    /**
     * 运行中
     */
    RUNNING,
    /**
     * 停止
     */
    STOP,
    /**
     * 关闭
     */
    SHUTDOWN
}
