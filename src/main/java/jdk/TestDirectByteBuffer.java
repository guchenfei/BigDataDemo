package jdk;

import java.nio.ByteBuffer;

public class TestDirectByteBuffer {
    public static void main(String[] args) {
        //开辟一块大小为10M的堆外内存
        ByteBuffer buffer = ByteBuffer.allocateDirect(10 * 1024 * 1024);
    }
}
