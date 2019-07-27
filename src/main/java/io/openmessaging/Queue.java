package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class Queue {
    /**
     * 队列的编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 直接内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    /**
     * 消息总数
     */
    private Long index = 0L;

    private boolean inited = false;

    private Long copied = 0L;

    private Long readed = 0L;

    public Queue(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.MESSAGE_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        buffer.putLong(message.getT());
        buffer.putLong(message.getA());
        buffer.put(message.getBody());
        index++;
    }

    public Message copy() {
        if (!inited) {
            int remain = buffer.remaining();
            if (remain > 0) {
                buffer.flip();
                try {
                    fileChannel.write(buffer);
                    buffer.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            inited = true;
        }
        if (readed >= index) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
        if (readed.equals(copied)) {
            try {
                buffer.clear();
                fileChannel.read(buffer, copied * Constants.MESSAGE_SIZE);
                buffer.flip();
                copied += Constants.MESSAGE_NUM;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long time = buffer.getLong();
        long value = buffer.getLong();
        byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
        buffer.get(body);
        readed++;
        return new Message(value, time, body);
    }
}
