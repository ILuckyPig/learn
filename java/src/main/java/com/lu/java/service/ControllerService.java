package com.lu.java.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class ControllerService {
    @Autowired
    ThreadService threadService;

    public void testFuture() throws ExecutionException, InterruptedException {
        Future<Integer> future = threadService.testFuture();
    }
}
