package com.tbox.process.support;

import com.tbox.process.exception.ExecutorException;
import com.tbox.process.type.LimitVelocityStrategy;

/**
 * 执行器属性配置
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public class ExecutorProperties {
    /**
     * 数据队列大小
     */
    private int queueSize = 1024;
    /**
     * 核心Worker数
     */
    private int coreWorkerSize = 1;
    /**
     * 最大Worker数
     */
    private int maxWorkerSize = 5;

    /**
     * fullBack之后尝试继续放入队列时间间隔
     * 单位：毫秒
     */
    private long fullBackOfferTimeout = 500;

    /**
     * 工作者拉取数据超时时间
     * 单位：毫秒
     */
    private long workerPullTimeout = 500;

    /**
     * 空闲Worker存活时间
     * 单位：毫秒
     */
    private long workerAlivetime = 20 * 1000;

    /**
     * 限速策略；默认平衡策略
     */
    private LimitVelocityStrategy limitVelocityStrategy = LimitVelocityStrategy.BALANCE;


    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getCoreWorkerSize() {
        return coreWorkerSize;
    }

    public void setCoreWorkerSize(int coreWorkerSize) {
        this.coreWorkerSize = coreWorkerSize;
    }

    public int getMaxWorkerSize() {
        return maxWorkerSize;
    }

    public void setMaxWorkerSize(int maxWorkerSize) {
        this.maxWorkerSize = maxWorkerSize;
    }

    public long getFullBackOfferTimeout() {
        return fullBackOfferTimeout > 0 ? fullBackOfferTimeout : 500; //当指定了fullBackOfferInterval则返回，否则返回默认100毫秒
    }

    public void setFullBackOfferTimeout(long fullBackOfferTimeout) {
        this.fullBackOfferTimeout = fullBackOfferTimeout;
    }

    public long getWorkerPullTimeout() {
        return workerPullTimeout > 0 ? workerPullTimeout : 500; //当指定了fullBackOfferInterval则返回，否则返回默认100毫秒
    }

    public void setWorkerPullTimeout(long workerPullTimeout) {
        this.workerPullTimeout = workerPullTimeout;
    }

    public long getWorkerAlivetime() {
        return workerAlivetime > 0 ? workerAlivetime : 20 * 1000; //当指定了fullBackOfferInterval则返回，否则返回默认20s;
    }

    public void setWorkerAlivetime(long workerAlivetime) {
        this.workerAlivetime = workerAlivetime;
    }

    public LimitVelocityStrategy getLimitVelocityStrategy() {
        return limitVelocityStrategy;
    }

    public void setLimitVelocityStrategy(LimitVelocityStrategy limitVelocityStrategy) {
        this.limitVelocityStrategy = limitVelocityStrategy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ExecutorProperties processProperties = new ExecutorProperties();

        public Builder queueSize(int queueSize) {
            processProperties.setQueueSize(queueSize);
            return this;
        }

        public Builder coreWorkerSize(int coreWorkerSize) {
            processProperties.setCoreWorkerSize(coreWorkerSize);
            return this;
        }

        public Builder maxWorkerSize(int maxWorkerSize) {
            if (maxWorkerSize > 256) {
                throw new ExecutorException("maxWorkerSize max 256");
            }
            processProperties.setMaxWorkerSize(maxWorkerSize);
            return this;
        }

        public Builder fullBackOfferTimeout(long fullBackOfferTimeout) {
            processProperties.setFullBackOfferTimeout(fullBackOfferTimeout);
            return this;
        }

        public Builder workerPullTimeout(long workerPullTimeout) {
            processProperties.setWorkerPullTimeout(workerPullTimeout);
            return this;
        }

        public Builder workerAlivetime(long workerAlivetime) {
            processProperties.setWorkerAlivetime(workerAlivetime);
            return this;
        }

        public Builder limitVelocityStrategy(LimitVelocityStrategy limitVelocityStrategy) {
            processProperties.setLimitVelocityStrategy(limitVelocityStrategy);
            return this;
        }

        public ExecutorProperties build() {
            return processProperties;
        }
    }

}
