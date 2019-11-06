package com.lu.java.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
public class ThreadService {

    @Async
    public Future<Integer> testFuture() {
        int result = 0;
        while (result < Integer.MAX_VALUE - 1) {
            result += 1;
            System.out.println(Thread.currentThread().getThreadGroup() + ", " + Thread.currentThread().getName() + ", result: " + result);
        }
        return new AsyncResult<>(result);
    }

    @Async
    public Future<Integer> testInterrupt() {
        int result = 0;
        while (result < Integer.MAX_VALUE - 1) {
            if (Thread.currentThread().isInterrupted()) {
                return new AsyncResult<>(-1);
            }
            result += 1;
            System.out.println(Thread.currentThread().getThreadGroup() + ", " + Thread.currentThread().getName() + ", result: " + result);
        }
        return new AsyncResult<>(result);
    }

    @Async
    public CompletableFuture<Integer> testCompletableFuture() {
        int result = 0;
        while (result < Integer.MAX_VALUE - 1) {
            if (Thread.currentThread().isInterrupted()) {
                return CompletableFuture.completedFuture(-1);
            }
            result += 1;
            System.out.println(Thread.currentThread().getThreadGroup() + ", " + Thread.currentThread().getName() + ", result: " + result);
        }
        return CompletableFuture.completedFuture(result);
    }
}
