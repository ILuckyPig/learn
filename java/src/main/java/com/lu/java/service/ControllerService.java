package com.lu.java.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
public class ControllerService {
    @Autowired
    ThreadService threadService;

    public void testFuture() {
        Future<Integer> future = threadService.testFuture();
    }

    public void testFuture(Boolean flag) {
        Future<Integer> future = threadService.testFuture();
        try {
            Thread.sleep(10);
            future.cancel(flag);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testInterrupt() {
        Future<Integer> future = threadService.testInterrupt();
        try {
            Thread.sleep(10);
            future.cancel(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testCompletableFuture() {
        CompletableFuture<Integer> future = threadService.testCompletableFuture();
        try {
            Thread.sleep(10);
            future.cancel(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
