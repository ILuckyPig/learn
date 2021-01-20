package com.lu.flink.kafka.producer.datastream.backpressure;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.time.LocalDateTime;

public class BackPressureSourceDemo {
    public void run() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    ByteBuf out = ctx.alloc().buffer();

                                    System.out.println("--> channel active");

                                    scheduleSendMessage(out, ctx);
                                }

                                public void scheduleSendMessage(ByteBuf out, ChannelHandlerContext ctx) throws InterruptedException {
                                    while (true) {
                                        String message = 1 + "," + LocalDateTime.now() + System.getProperty("line.separator");
                                        System.out.print("<-- send: " + message);
                                        out.writeBytes(message.getBytes()).retain();
                                        ctx.writeAndFlush(out);
                                        out.clear();
                                        Thread.sleep(300);
                                    }
                                }
                            });
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture future = bootstrap.bind(10234).sync();
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new BackPressureSourceDemo().run();
    }
}
