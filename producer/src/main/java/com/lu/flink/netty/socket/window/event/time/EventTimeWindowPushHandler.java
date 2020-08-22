package com.lu.flink.netty.socket.window.event.time;

import com.lu.flink.netty.socket.PushHandler;

import java.time.LocalDateTime;
import java.util.Random;

public class EventTimeWindowPushHandler extends PushHandler {
    private Random random = new Random();
    public EventTimeWindowPushHandler(int gap) {
        setGap(gap);
    }

    @Override
    public String produceMessage() {
        LocalDateTime now = LocalDateTime.now().minusSeconds(random.nextInt(5));
        return KEY + "," + now;
    }
}
