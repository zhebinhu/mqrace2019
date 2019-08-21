package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;

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
    private static FileChannel fileChannel;

    /**
     * 堆外内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_NUM);

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;


    static{
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + "100.value", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
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
        buffer.putLong(message.getA());
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
    }

    public long get(int index, ValueContext valueContext) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        if (index >= valueContext.bufferMinIndex && index < valueContext.bufferMaxIndex) {
            valueContext.buffer.position((index - valueContext.bufferMinIndex) * Constants.VALUE_SIZE);
        } else {
            valueContext.buffer.clear();
            try {
                fileChannel.read(valueContext.buffer, ((long) index) * Constants.VALUE_SIZE);
                valueContext.bufferMinIndex = index;
                valueContext.bufferMaxIndex = Math.min(index + Constants.VALUE_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            valueContext.buffer.flip();
        }
        return valueContext.buffer.getLong();
    }

}