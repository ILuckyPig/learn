package com.lu.flink.netty.socket.late.events;

import com.lu.flink.netty.socket.PushHandler;

import java.time.LocalDateTime;

public class LateEventsPushHandler extends PushHandler {
    @Override
    public String produceMessage() {
        LocalDateTime now = LocalDateTime.now();
        return KEY + "," + now;
    }
}
