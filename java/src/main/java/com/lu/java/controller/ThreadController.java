package com.lu.java.controller;

import com.lu.java.service.ControllerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ThreadController {
    @Autowired
    ControllerService controllerService;

    @RequestMapping(value = "/testFuture", method = RequestMethod.GET)
    public String testFuture() {
        controllerService.testFuture();
        return "1";
    }

    @RequestMapping(value = "/testCancel", method = RequestMethod.GET)
    public String testCancel() {
        controllerService.testFuture(true);
        return "1";
    }

    @RequestMapping(value = "/testInterrupt", method = RequestMethod.GET)
    public String testInterrupt() {
        controllerService.testInterrupt();
        return "1";
    }

    @RequestMapping(value = "/testCompletableFuture", method = RequestMethod.GET)
    public String testCompletableFuture() {
        controllerService.testCompletableFuture();
        return "1";
    }
}
