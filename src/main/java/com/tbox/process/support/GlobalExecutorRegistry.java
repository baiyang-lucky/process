package com.tbox.process.support;

import com.tbox.process.Master;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.WeakHashMap;

/**
 * 全局Executor注册器
 * 确保安全性只允许包可见不允许修改
 *
 * @author 白杨
 * DateTime:2023/8/23 10:43
 */
final class GlobalExecutorRegistry {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExecutorRegistry.class);
    /**
     * 执行器登记，用于在监控时获取当前整个应用的所有Master任务信息。
     * 使用WeakHashMap，当Master关闭回收之后，自动回收其ExecutorContext对象，防止内存泄漏。
     * 包可见。
     */
    final static WeakHashMap<Master<?>, ExecutorContext> registry = new WeakHashMap<>();

    public static void register(Master<?> master, ExecutorContext context) {
        logger.info("Excustor {} registered.", master.getName());
        registry.put(master, context);
    }


}
