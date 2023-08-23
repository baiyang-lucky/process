package com.tbox.process;

import com.tbox.process.support.ControllableMsterExecutor;
import com.tbox.process.support.ExecutorProperties;

import java.util.ArrayList;
import java.util.Random;

public class ProcessTest {
    public static void main(String[] args) throws InterruptedException {

        ControllableMsterExecutor<Book> printTask = new ControllableMsterExecutor.Builder<Book>().name("print_task")
                .processProperties(ExecutorProperties.builder()
                        .queueSize(500)
                        .coreWorkerSize(1)
                        .maxWorkderSize(5)
                        .build())
                .masterPuller(() -> {
                    ArrayList<Book> books = new ArrayList<>();
                    books.add(new Book("baiyang"));
                    System.out.println(Thread.currentThread().getName() + "拉取数据...");
                    return books;
                })
                .workerConsumer((data) -> {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + ":" + data);
                })
                .build();
        printTask.start();
//        Thread.sleep(1000);
        printTask.stop();
//        System.out.println();
//        Scanner scanner = new Scanner(System.in);
//        scanner.next();
//        printTask.stop();
//        scanner.next();
//        printTask.start();
//        scanner.next();
//        printTask.stopNow();
//        scanner.next();
//        printTask.shutdown();
    }
}

class Book {
    public String name;

    public Book(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Book{" +
                "name='" + name + '\'' +
                '}';
    }
}
