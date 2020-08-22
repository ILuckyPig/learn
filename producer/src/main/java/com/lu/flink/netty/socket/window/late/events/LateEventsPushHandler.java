package com.lu.flink.netty.socket.window.late.events;

import com.lu.flink.netty.socket.PushHandler;

import java.time.LocalDateTime;
import java.util.Random;

public class LateEventsPushHandler extends PushHandler {
    private Random random = new Random();
    public LateEventsPushHandler(int gap) {
        setGap(gap);
    }

    @Override
    public String produceMessage() {
        LocalDateTime now = LocalDateTime.now().minusSeconds(random.nextInt(10));
        return KEY + "," + now;
    }
}
