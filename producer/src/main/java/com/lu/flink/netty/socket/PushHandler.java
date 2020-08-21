package com.lu.flink.netty.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public abstract class PushHandler extends ChannelInboundHandlerAdapter {
    public static final String KEY = "key";
    public static String WARP = System.getProperty("line.separator");
    private int gap = 5;

    public int getGap() {
        return gap;
    }

    public void setGap(int gap) {
        this.gap = gap;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteBuf out = ctx.alloc().buffer();

        System.out.println("--> channel active");

        scheduleSendMessage(out, ctx);
    }

    public abstract String produceMessage();

    public void scheduleSendMessage(ByteBuf out, ChannelHandlerContext ctx) throws InterruptedException {
        while (true) {
            String message = produceMessage() + WARP;

            System.out.print("<-- send: " + message);

            out.writeBytes(message.getBytes()).retain();
            ctx.writeAndFlush(out);

            out.clear();
            Thread.sleep(1000 * gap);
        }
    }
}
