package com.tbox.process;

public interface Worker<T> extends Runnable {
    void start();
}
