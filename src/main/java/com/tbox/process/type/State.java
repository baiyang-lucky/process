package com.tbox.process.type;

/**
 * 状态
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public enum State {
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
