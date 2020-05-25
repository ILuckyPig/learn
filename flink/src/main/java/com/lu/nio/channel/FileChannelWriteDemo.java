package com.lu.nio.channel;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * flink network nio
 */
public class FileChannelWriteDemo {
    public static void main(String[] args) throws IOException {
        URL resource = FileChannelReadDemo.class.getResource("/");
        RandomAccessFile aFile = new RandomAccessFile(resource.getPath() + "test.properties", "rw");
        String[] strings = {"hello", "world"};
        FileChannel channel = aFile.getChannel();

        ByteBuffer buffer = ByteBuffer.allocate(48);

        for (String string : strings) {
            buffer.clear();
            buffer.put(string.getBytes());
            buffer.flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        channel.close();
        aFile.close();
    }
}
