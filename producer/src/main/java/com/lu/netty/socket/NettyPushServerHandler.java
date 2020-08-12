package com.lu.netty.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyPushServerHandler extends ChannelInboundHandlerAdapter {
    ScheduleProducer producer = new ScheduleProducer();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteBuf out = ctx.alloc().buffer();

        System.out.println("--> channel active");

        producer.scheduleSendMessage(out, ctx);
    }
}
