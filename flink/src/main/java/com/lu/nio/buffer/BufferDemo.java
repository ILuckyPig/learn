package com.lu.nio.buffer;

import java.nio.IntBuffer;

/**
 * flink network nio
 */
public class BufferDemo {
    public static void main(String[] args) {
        IntBuffer intBuffer = IntBuffer.allocate(2);
        intBuffer.put(12345678);
        intBuffer.put(2);
        // intBuffer.put(1);  BufferOverflowException
        intBuffer.flip();
        System.out.println(intBuffer.get());
        System.out.println(intBuffer.get());
    }
}
