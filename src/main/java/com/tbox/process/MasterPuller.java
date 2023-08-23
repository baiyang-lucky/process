package com.tbox.process;

import java.util.List;

/**
 * 主节点的数据拉取器，在任务执行过程中拉取数据是持续的，所以该类实现需要维持拉取进度状态，避免导致重复获取相同数据反复消费。
 * pull方法如果阻塞，Master的主逻辑执行也将阻塞。
 * @param <T>
 */
@FunctionalInterface
public interface MasterPuller<T> {

    /**
     * 拉取数据，返回的数据将会传入数据队列传输给worker
     */
    List<T> pull();
}
