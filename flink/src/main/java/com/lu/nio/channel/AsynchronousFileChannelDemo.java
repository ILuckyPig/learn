package com.lu.nio.channel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;

/**
 * async file channel from jdk1.7
 */
public class AsynchronousFileChannelDemo {
    public static void main(String[] args) throws IOException, URISyntaxException {
        URL resource = FileChannelReadDemo.class.getResource("/log4j.properties");
        System.out.println(Paths.get(resource.toURI()).toString());
        Path path = Paths.get(Paths.get(resource.toURI()).toString());
        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        Future<Integer> read = asynchronousFileChannel.read(buffer, 0);
        while (!read.isDone()) {
            buffer.flip();
            byte[] data = new byte[buffer.limit()];
            buffer.get(data);
            System.out.println(new String(data));
            buffer.clear();
        }

        // TODO http://ifeve.com/java-nio-asynchronousfilechannel/

    }
}
