package com.tbox.process;

import com.tbox.process.type.EventState;

import java.util.concurrent.LinkedBlockingDeque;

class ProcessApplicationTests {
    public static void test(EventState state){
        state = EventState.RUN;
    }
    public static void main(String[] args) throws InterruptedException {
//        EventState state = EventState.STOP;
//        test(state);
//        System.out.println(state);
//        ReentrantLock lock = new ReentrantLock();
//        Condition runNotify = lock.newCondition();
//        Condition stopNotify = lock.newCondition();
//
//        Thread thread = new Thread(() -> {
//            try {
//                Thread.sleep(1);
//                lock.lock();
//                System.out.println("等待停止");
//                stopNotify.await();
//                System.out.println("获得通知");
//                lock.lock();
//                runNotify.signalAll();
//                lock.unlock();
//                System.out.println("通知运行");
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            } finally {
//
//            }
//        });
//        thread.start();
//
//        Thread.sleep(1000);
//        try {
//            lock.lock();
//            stopNotify.signalAll();
//            System.out.println("await");
//            runNotify.await();
//        } finally {
//            System.out.println("awaitEnd");
//        }
        LinkedBlockingDeque q = new LinkedBlockingDeque();
        q.offer(1);
        System.out.println(q.poll());
        System.out.println(q.poll());
    }

    static int workerIdFlag = 0;

    public static int getWokerId() {
        int i = 0;
        for (; ((workerIdFlag >> i) & 1) != 0; ) {
            i++;
        }
        workerIdFlag |= 1 << i;
//        workerIdFlag&=~(1<<1)
        return i + 1;
    }
}
