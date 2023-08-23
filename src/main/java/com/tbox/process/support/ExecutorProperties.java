package com.tbox.process.support;

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
    private int maxWorkderSize = 5;

    /**
     * fullBack之后尝试继续放入队列时间间隔
     * 单位：毫秒
     */
    private int fullBackOfferTimeout = 500;

    /**
     * 工作者拉取数据超时时间
     * 单位：毫秒
     */
    private int workerPullTimeout = 500;

    /**
     * 空闲Woker存活时间
     */
    private int workerAlivetime = 20 * 1000;


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

    public int getMaxWorkderSize() {
        return maxWorkderSize;
    }

    public void setMaxWorkderSize(int maxWorkderSize) {
        this.maxWorkderSize = maxWorkderSize;
    }

    public int getFullBackOfferTimeout() {
        return fullBackOfferTimeout > 0 ? fullBackOfferTimeout : 500; //当指定了fullBackOfferInterval则返回，否则返回默认100毫秒
    }

    public void setFullBackOfferTimeout(int fullBackOfferTimeout) {
        this.fullBackOfferTimeout = fullBackOfferTimeout;
    }

    public int getWorkerPullTimeout() {
        return workerPullTimeout > 0 ? workerPullTimeout : 500; //当指定了fullBackOfferInterval则返回，否则返回默认100毫秒
    }

    public void setWorkerPullTimeout(int workerPullTimeout) {
        this.workerPullTimeout = workerPullTimeout;
    }

    public int getWorkerAlivetime() {
        return workerAlivetime > 0 ? workerAlivetime : 20 * 1000; //当指定了fullBackOfferInterval则返回，否则返回默认20s;
    }

    public void setWorkerAlivetime(int workerAlivetime) {
        this.workerAlivetime = workerAlivetime;
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

        public Builder maxWorkderSize(int maxWorkderSize) {
            processProperties.setMaxWorkderSize(maxWorkderSize);
            return this;
        }

        public Builder fullBackOfferTimeout(int fullBackOfferTimeout) {
            processProperties.setFullBackOfferTimeout(fullBackOfferTimeout);
            return this;
        }

        public Builder workerPullTimeout(int workerPullTimeout) {
            processProperties.setWorkerPullTimeout(workerPullTimeout);
            return this;
        }

        public Builder workerAlivetime(int workerAlivetime) {
            processProperties.setWorkerAlivetime(workerAlivetime);
            return this;
        }

        public ExecutorProperties build() {
            return processProperties;
        }
    }

}
