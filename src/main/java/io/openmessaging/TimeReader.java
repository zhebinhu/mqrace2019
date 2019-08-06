package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class TimeReader {
    /**
     * 编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 堆内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.TIME_PAGE_SIZE);

    private volatile boolean inited = false;

    int i = 0;

    TimePage timePage = new TimePage();

    int k = 0;

    int pageIndex = 0;

    LRUCache<Integer, TimePage> pageCache = new LRUCache<>(Constants.TIME_CACHE_SIZE / Constants.TIME_PAGE_SIZE);

    public TimeReader(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".time", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(byte t) {
        if (i == Constants.TIME_PAGE_SIZE) {
            pageCache.put(k, timePage);
            k++;
            timePage = new TimePage();
            i = 0;
        }
        if (k < Constants.TIME_CACHE_SIZE / Constants.TIME_PAGE_SIZE) {
            timePage.bytes[i] = t;
            i++;
        }

        if (!buffer.hasRemaining()) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.clear();
        }
        buffer.put(t);
    }

    public void init() {
        int remain = buffer.remaining();
        pageIndex = -1;
        timePage = null;
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

    public byte getTime(int offset) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        if (pageIndex == offset / Constants.TIME_PAGE_SIZE) {
            return timePage.bytes[offset % Constants.TIME_PAGE_SIZE];
        }
        pageIndex = offset / Constants.TIME_PAGE_SIZE;

        timePage = pageCache.get(pageIndex);

        if (timePage == null) {
            try {
                buffer.clear();
                fileChannel.read(buffer, pageIndex * Constants.TIME_PAGE_SIZE);
                buffer.flip();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            timePage = pageCache.getOldest();
            if (timePage == null) {
                timePage = new TimePage();
            }
            buffer.get(timePage.bytes, 0, buffer.limit());
            pageCache.put(pageIndex, timePage);
        }

        return timePage.bytes[offset % Constants.TIME_PAGE_SIZE];
    }

}