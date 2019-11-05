package com.lu.java.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
public class ThreadService {

    @Async
    public Future<Integer> testFuture() throws InterruptedException {
        int result = 0;
        for (int i = 0; i < 10; i++) {
            result += i;
            System.out.println(Thread.currentThread().getThreadGroup() + ", " + Thread.currentThread().getName() + ", result: " + result);
            Thread.sleep(1000);
        }
        return new AsyncResult<>(result);
    }
}
