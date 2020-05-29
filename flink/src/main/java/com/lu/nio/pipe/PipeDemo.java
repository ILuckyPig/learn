package com.lu.nio.pipe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;

public class PipeDemo {
    public static void main(String[] args) throws IOException {
        Pipe pipe = Pipe.open();
        Pipe.SinkChannel sink = pipe.sink();
        String newData = "New String to write to file..." + System.currentTimeMillis();
        // write data to sink channel
        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.clear();
        buf.put(newData.getBytes());
        buf.flip();
        while (buf.hasRemaining()) {
            sink.write(buf);
        }

        // read data from sink channel
        Pipe.SourceChannel source = pipe.source();
        ByteBuffer readBuff = ByteBuffer.allocate(48);
        int read = source.read(readBuff);
        System.out.println(new String(readBuff.array(), 0, read));
        source.close();
        sink.close();
    }
}
