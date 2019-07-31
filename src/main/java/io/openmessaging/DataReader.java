package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
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

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * 异步put结果
     */
    private Future putFuture;

    public DataReader(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
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
                e.printStackTrace();
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

    public synchronized void getData(long index, Message message) {

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }

        if (index >= bufferMinIndex && index < bufferMaxIndex) {
            buffer.position((int) (index - bufferMinIndex) * Constants.DATA_SIZE);
        } else {
            buffer.clear();
            try {
                fileChannel.read(buffer, index * Constants.DATA_SIZE);
                bufferMinIndex = index;
                bufferMaxIndex = Math.min(index + Constants.DATA_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.flip();
        }

        buffer.get(message.getBody());
    }

}