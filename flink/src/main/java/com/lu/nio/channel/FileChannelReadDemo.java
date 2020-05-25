package com.lu.nio.channel;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * flink network nio
 */
public class FileChannelReadDemo {
    public static void main(String[] args) throws IOException {
        URL resource = FileChannelReadDemo.class.getResource("/");
        RandomAccessFile aFile = new RandomAccessFile(resource.getPath() + "log4j.properties", "rw");
        FileChannel inChannel = aFile.getChannel();

        ByteBuffer buf = ByteBuffer.allocate(48);

        int bytesRead = inChannel.read(buf);
        while (bytesRead != -1) {
            buf.flip();

            while (buf.hasRemaining()) {
                System.out.print((char) buf.get());
            }

            buf.clear();
            bytesRead = inChannel.read(buf);
        }
        inChannel.close();
        aFile.close();
    }
}
