package com.lu.flink.netty.socket.window.incremental.aggregation;

import com.lu.flink.netty.socket.PushHandler;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

public class ProcessWindowPushHandler extends PushHandler {
    Random random = new Random();
    @Override
    public String produceMessage() {
        long epochSecond = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return KEY + "," + epochSecond + "," + random.nextInt(10);
    }
}
