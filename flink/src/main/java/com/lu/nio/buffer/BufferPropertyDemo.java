package com.lu.nio.buffer;

import scala.Int;

import java.nio.IntBuffer;

public class BufferPropertyDemo {
    public static void main(String[] args) {
        IntBuffer intBuffer = IntBuffer.allocate(10);
        intBuffer.put(10);
        intBuffer.put(101);
        System.out.println("Write mode: ");
        System.out.println("\tCapacity: " + intBuffer.capacity());
        System.out.println("\tPosition: " + intBuffer.position());
        System.out.println("\tLimit: " + intBuffer.limit());

        intBuffer.flip();
        System.out.println("Read mode: ");
        System.out.println("\tCapacity: " + intBuffer.capacity());
        System.out.println("\tPosition: " + intBuffer.position());
        System.out.println("\tLimit: " + intBuffer.limit());

        System.out.println(intBuffer.get());
        System.out.println("Read mode: ");
        System.out.println("\tCapacity: " + intBuffer.capacity());
        System.out.println("\tPosition: " + intBuffer.position());
        System.out.println("\tLimit: " + intBuffer.limit());
    }
}
