package com.lu.nio.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * flink network nio
 *  nc -l 9999
 *  hello world
 */
public class SocketChannelDemo {
    public static void main(String[] args) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("localhost", 9999));

        ByteBuffer buffer = ByteBuffer.allocate(48);
        int read = channel.read(buffer);
        buffer.flip();
        while (buffer.hasRemaining()) {
            System.out.print((char) buffer.get());
        }
        buffer.clear();

        String hi = "hi";
        buffer.put(hi.getBytes());
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        channel.close();
    }
}
