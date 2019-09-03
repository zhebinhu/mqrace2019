package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author huzb
 * @version v1.0.0
 * @date 2019/9/3
 */
public class TmpWriter {
    private ByteBuffer buffer = ByteBuffer.allocateDirect(50 * 1024 * 1024);

    private FileChannel fileChannel;

    private long msgNum = 0;

    private long offsetA = 0;

    private long offsetB = 0;

    private Message message = new Message(0, 0, new byte[34]);

    private boolean inited = false;

    public void init() {
        int remain = buffer.remaining();
        if (remain > 0) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }
    }

    public TmpWriter() {
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + "tmp.msg", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        buffer.putLong(message.getT());
        buffer.putLong(message.getA());
        buffer.put(message.getBody());
        if (!buffer.hasRemaining()) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.clear();
        }
        msgNum++;
    }

    public Message get() {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        if (offsetA == msgNum) {
            return null;
        }
        if (offsetA == offsetB) {
            buffer.clear();
            try {
                fileChannel.read(buffer, offsetA * 50);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.flip();
            offsetB += buffer.limit() / 50;
        }
        offsetA++;
        message.setT(buffer.getLong());
        message.setA(buffer.getLong());
        buffer.get(message.getBody());
        return message;
    }
}
