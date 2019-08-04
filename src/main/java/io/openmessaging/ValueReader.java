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
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_PAGE_SIZE * Constants.VALUE_SIZE);

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

    int i = 0;

    ValuePage valuePage = new ValuePage();

    int k = 0;

    int pageIndex = 0;

    LRUCache<Integer, ValuePage> pageCache = new LRUCache<>(Constants.VALUE_CACHE_SIZE / Constants.VALUE_PAGE_SIZE);

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
        long t = message.getA() - message.getT();
        if (i == Constants.VALUE_PAGE_SIZE) {
            pageCache.put(k, valuePage);
            k++;
            valuePage = new ValuePage();
            i = 0;
        }
        if (k < Constants.VALUE_CACHE_SIZE / Constants.VALUE_PAGE_SIZE) {
            valuePage.ints[i] = (int) t;
            i++;
        }

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

        buffer.putInt((int) t);
        messageNum++;
    }

    public void init() {
        int remain = buffer.remaining();
        pageIndex = -1;
        valuePage = null;
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

    public int getValue(int index) {

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }

        //        if (index >= bufferMinIndex && index < bufferMaxIndex) {
        //            buffer.position((int) (index - bufferMinIndex) * Constants.VALUE_SIZE);
        //        } else {
        //            buffer.clear();
        //            try {
        //                fileChannel.read(buffer, index * Constants.VALUE_SIZE);
        //                bufferMinIndex = index;
        //                bufferMaxIndex = Math.min(index + Constants.VALUE_NUM, messageNum);
        //            } catch (IOException e) {
        //                e.printStackTrace(System.out);
        //            }
        //            buffer.flip();
        //        }
        //
        //        return buffer.getInt();
        if (pageIndex == index / Constants.VALUE_PAGE_SIZE) {
            return valuePage.ints[index % Constants.VALUE_PAGE_SIZE];
        }
        pageIndex = index / Constants.VALUE_PAGE_SIZE;

        valuePage = pageCache.get(pageIndex);

        if (valuePage == null) {
            try {
                buffer.clear();
                fileChannel.read(buffer, pageIndex * Constants.VALUE_PAGE_SIZE);
                if (buffer.limit() != Constants.VALUE_PAGE_SIZE) {
                    System.out.println("buffer size=" + buffer.limit());
                }
                buffer.flip();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            valuePage = pageCache.getOldest();
            if (valuePage == null) {
                valuePage = new ValuePage();
            }

            int p = 0;
            while (buffer.hasRemaining()) {
                valuePage.ints[p++] = buffer.getInt();
            }
            pageCache.put(pageIndex, valuePage);
        }

        return valuePage.ints[index % Constants.VALUE_PAGE_SIZE];
    }

}