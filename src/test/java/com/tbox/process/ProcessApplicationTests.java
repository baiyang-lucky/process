package com.tbox.process;

import com.tbox.process.type.Event;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

class ProcessApplicationTests {
    public static void test(Event state) {
        state = Event.RUN;
    }

    public static void main(String[] args) throws InterruptedException {
        Thread t = new Thread(() -> {
            while (true) {
                System.out.println(new Random().nextInt(100));
            }
        });
        t.start();
        Thread.sleep(1000);
        t.stop();
//        while (true)
//            t.interrupt();
//        System.out.println("触发中断");
        long l = Long.MAX_VALUE;
//        System.out.println();
//        Thread.sleep(1000);
        System.out.println(TimeUnit.MILLISECONDS.toSeconds(l));


    }

    static int workerIdFlag = 0;

    public static int getWorkerId() {
        int i = 0;
        for (; ((workerIdFlag >> i) & 1) != 0; ) {
            i++;
        }
        workerIdFlag |= 1 << i;
//        workerIdFlag&=~(1<<1)
        return i + 1;
    }
}
