package com.tbox.process.support;

import com.tbox.process.exception.ExecutorException;

/**
 * WorkerId池
 */
public class WorkerIdPool {
    private static final int BYTE_BIT_SIZE = 8;
    private final byte[] workerIdPool = new byte[32]; // 最多256个workerId
    private final Object workerIdLock = new Object();

    /**
     * 获取workerId
     */
    public int getWorkerId() {
        int workerId = 0;
        synchronized (workerIdLock) {
            for (int j = 0; j < workerIdPool.length; j++) {
                byte b = workerIdPool[j];
                int i = 0;
                while ((((((int) b) & 0xff) >>> i) & 1) != 0) {
                    i++;
                    workerId++;
                }
                if (i != BYTE_BIT_SIZE) {
                    workerIdPool[j] |= 1 << i;
                    break;
                }
            }
        }
        if (workerId >= workerIdPool.length * BYTE_BIT_SIZE) {
            throw new ExecutorException("workerId pool full.");
        }
        return workerId + 1;
    }

    /**
     * 释放归还workerId
     */
    public void releaseWorkerId(int workerId) {
        synchronized (workerIdLock) {
//            workerIdPool[0]&=(~(1<<7))&0xff
            workerIdPool[((workerId + BYTE_BIT_SIZE - 1) / BYTE_BIT_SIZE) - 1] &= (~(1 << ((workerId - 1) % BYTE_BIT_SIZE))) & 0xff;
        }
    }
}
