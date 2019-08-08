package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Message;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class DataReader {
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
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    private ThreadLocal<ByteBuffer> readBuffer = new ThreadLocal<>();

    private ThreadLocal<Integer> bufferMaxIndex = new ThreadLocal<>();

    private ThreadLocal<Integer> bufferMinIndex = new ThreadLocal<>();

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;


    public DataReader() {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + "100.data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.DATA_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.clear();
        }
        buffer.put(message.getBody());
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

    public void getData(int index, Message message) {

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        if (bufferMinIndex.get() == null) {
            bufferMinIndex.set(-1);
        }
        if (bufferMaxIndex.get() == null) {
            bufferMaxIndex.set(-1);
        }
        ByteBuffer tmpBuffer;
        if (readBuffer.get() == null) {
            tmpBuffer = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);
            readBuffer.set(tmpBuffer);
        }else {
            tmpBuffer = readBuffer.get();
        }
        if (index >= bufferMinIndex.get() && index < bufferMaxIndex.get()) {
            tmpBuffer.position((index - bufferMinIndex.get()) * Constants.DATA_SIZE);
        } else {
            tmpBuffer.clear();
            try {
                fileChannel.read(tmpBuffer, ((long) index) * Constants.DATA_SIZE);
                bufferMinIndex.set(index);
                bufferMaxIndex.set(Math.min(index + Constants.DATA_NUM, messageNum));
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            tmpBuffer.flip();
        }

        tmpBuffer.get(message.getBody());
    }

}