package com.lu.netty.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ScheduleProducer {
    private static final String KEY = "key";
    public static String WARP = System.getProperty("line.separator");

    public String produceMessage() {
        long epochSecond = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return KEY + "," + epochSecond + WARP;
    }

    public void scheduleSendMessage(ByteBuf out, ChannelHandlerContext ctx) throws InterruptedException {
        while (true) {
            String message = produceMessage();

            System.out.print("<-- send: " + message);

            out.writeBytes(message.getBytes()).retain();
            ctx.writeAndFlush(out);

            out.clear();
            Thread.sleep(1000 * 5);
        }
    }
}
