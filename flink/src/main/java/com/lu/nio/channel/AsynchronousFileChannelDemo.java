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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * async file channel from jdk1.7
 */
public class AsynchronousFileChannelDemo {
    public static void main(String[] args) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        URL resource = FileChannelReadDemo.class.getResource("/log4j.properties");
        System.out.println(Paths.get(resource.toURI()).toString());
        Path path = Paths.get(Paths.get(resource.toURI()).toString());
        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        long length = path.toFile().length();
        ByteBuffer buffer = ByteBuffer.allocate(100);
        int i = 0;
        int readInt;
        while (i < length) {
            Future<Integer> read = asynchronousFileChannel.read(buffer, i);
            readInt = read.get();
            buffer.flip();
            while (buffer.hasRemaining()) {
                System.out.print((char) buffer.get());
            }
            buffer.clear();
            i += readInt;
        }
        asynchronousFileChannel.close();
        // TODO http://ifeve.com/java-nio-asynchronousfilechannel/

    }
}
