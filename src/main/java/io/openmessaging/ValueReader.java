package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class ValueReader {
    /**
     * 编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 堆外内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_NUM);

    /**
     * 消息总数
     */
    private long messageNum = 0L;

    /**
     * 缓存中最大消息
     */
    private long bufferMaxIndex = -1L;

    /**
     * 缓存中最小消息
     */
    private long bufferMinIndex = -1L;

    private volatile boolean inited = false;

    private long count = 0;

    private int tag = -1;

    public ValueReader(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".value", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        int value = (int) (message.getA() - message.getT());
        int remain = buffer.remaining();
        if (remain < Constants.VALUE_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.clear();
        }
        if (tag == -1 || value > tag + 15) {
            tag = value;
            count++;
            System.out.println("thread" + num + ":tag=" + tag);
        }
        buffer.putInt(value);
        messageNum++;
    }

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
        System.out.println("thread" + num + ":count=" + count);
    }

    public int getValue(int index) {

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }

        if (index >= bufferMinIndex && index < bufferMaxIndex) {
            buffer.position((int) (index - bufferMinIndex) * Constants.VALUE_SIZE);
        } else {
            buffer.clear();
            try {
                fileChannel.read(buffer, index * Constants.VALUE_SIZE);
                bufferMinIndex = index;
                bufferMaxIndex = Math.min(index + Constants.VALUE_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.flip();
        }

        return buffer.getInt();
    }

}