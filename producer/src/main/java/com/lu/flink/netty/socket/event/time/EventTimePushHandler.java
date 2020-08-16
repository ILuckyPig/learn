package com.lu.flink.netty.socket.event.time;

import com.lu.flink.netty.socket.PushHandler;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class EventTimePushHandler extends PushHandler {

    @Override
    public String produceMessage() {
        long epochSecond = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return KEY + "," + epochSecond;
    }
}
