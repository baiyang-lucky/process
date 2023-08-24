package com.tbox.process.type;

/**
 * 限速策略
 */
public enum LimitVelocityStrategy {
    /**
     * 不限速
     */
    NO_LIMIT,
    /**
     * 平衡生产与消费速度，使生产速度与最大消费速度匹配。
     */
    BALANCE
}
