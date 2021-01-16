package com.lu.nio.channel;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class AsynchronousFileChannelDemo2 {
    public static void main(String[] args) throws URISyntaxException, IOException {
        URL resource = FileChannelReadDemo.class.getResource("/log4j2-test.properties");
        System.out.println(Paths.get(resource.toURI()).toString());
        Path path = Paths.get(Paths.get(resource.toURI()).toString());
        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        ByteBuffer buffer = ByteBuffer.allocate(100);
        asynchronousFileChannel.read(buffer, 0, asynchronousFileChannel, new MyHandler(buffer));
        System.in.read();
    }

    static class MyHandler implements CompletionHandler<Integer, AsynchronousFileChannel> {
        final int[] pos = {0};
        ByteBuffer buffer;

        public MyHandler(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void completed(Integer result, AsynchronousFileChannel attachment) {
            if (result != -1) {
                pos[0] += result;
                buffer.flip();
                while (buffer.hasRemaining()) {
                    System.out.print((char) buffer.get());
                }
                buffer.clear();
            }
            attachment.read(buffer, pos[0], attachment, this);
        }

        @Override
        public void failed(Throwable exc, AsynchronousFileChannel attachment) {

        }
    }
}
