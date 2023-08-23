package com.tbox.process.type;

/**
 * 执行状态
 */
public enum EventState {
    /**
     * 运行
     */
    RUN,
    /**
     * 停止
     */
    STOP,
    /**
     * 立即停止
     */
    STOP_NOW,
    /**
     * 关闭
     */
    SHUTDOWN,
    /**
     * 立即关闭
     */
    SHUTDOWN_NOW
}
