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

    private ThreadLocal<ByteBuffer> readBuffer = new ThreadLocal<>();

    private ThreadLocal<Integer> pageIndex = new ThreadLocal<>();

    private ThreadLocal<TimePage> timePage = new ThreadLocal<>();

    private volatile boolean inited = false;

    int i = 0;

    int k = 0;

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
        if(timePage.get()==null){
            timePage.set(new TimePage());
        }
        if (i == Constants.TIME_PAGE_SIZE) {
            pageCache.put(k, timePage.get());
            k++;
            timePage.set(new TimePage());
            i = 0;
        }
        if (k < Constants.TIME_CACHE_SIZE / Constants.TIME_PAGE_SIZE) {
            timePage.get().bytes[i] = t;
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
        if (pageIndex.get() == null) {
            pageIndex.set(-1);
        }
        if (pageIndex.get() == offset / Constants.TIME_PAGE_SIZE) {
            return timePage.get().bytes[offset % Constants.TIME_PAGE_SIZE];
        }
        pageIndex.set(offset / Constants.TIME_PAGE_SIZE);

        timePage.set(pageCache.get(pageIndex.get()));

        if (timePage.get() == null) {
            System.out.println("time uncache");
            try {
                if (readBuffer.get() == null) {
                    readBuffer.set(ByteBuffer.allocateDirect(Constants.TIME_PAGE_SIZE));
                }
                readBuffer.get().clear();
                fileChannel.read(readBuffer.get(), pageIndex.get() * Constants.TIME_PAGE_SIZE);
                readBuffer.get().flip();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            timePage.set(new TimePage());
            readBuffer.get().get(timePage.get().bytes, 0, readBuffer.get().limit());
            pageCache.put(pageIndex.get(), timePage.get());
        }

        return timePage.get().bytes[offset % Constants.TIME_PAGE_SIZE];
    }

}