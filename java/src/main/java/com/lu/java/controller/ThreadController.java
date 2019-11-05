package com.lu.java.controller;

import com.lu.java.service.ControllerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class ThreadController {
    @Autowired
    ControllerService controllerService;

    @RequestMapping(value = "/testFuture", method = RequestMethod.GET)
    public String testFuture() throws ExecutionException, InterruptedException {
        controllerService.testFuture();
        return "1";
    }
}
