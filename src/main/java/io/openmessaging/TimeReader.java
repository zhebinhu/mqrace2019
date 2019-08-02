package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

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
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.PAGE_SIZE);

    private volatile boolean inited = false;

    int i = 0;

    Page page = new Page();

    int k = 0;

    int pageIndex = 0;

    LRUCache<Integer, Page> pageCache = new LRUCache<>(Constants.CACHE_SIZE / Constants.PAGE_SIZE);

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
//        if (i == Constants.PAGE_SIZE) {
//            pageCache.put(k, page);
//            k++;
//            page = new Page();
//            i = 0;
//        }
//        if (k < Constants.CACHE_SIZE / Constants.PAGE_SIZE) {
//            page.bytes[i] = t;
//            i++;
//        }

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
        page = null;
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
        if (pageIndex == offset / Constants.PAGE_SIZE) {
            return page.bytes[offset % Constants.PAGE_SIZE];
        }
        pageIndex = offset / Constants.PAGE_SIZE;

        page = pageCache.get(pageIndex);

        if (page == null) {
            try {
                buffer.clear();
                fileChannel.read(buffer, pageIndex * Constants.PAGE_SIZE);
                buffer.flip();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            page = pageCache.getOldest();
            if (page == null) {
                page = new Page();
            }
            buffer.get(page.bytes, 0, buffer.limit());
            pageCache.put(pageIndex, page);
        }

        offset = offset % Constants.PAGE_SIZE;
        return page.bytes[offset % Constants.PAGE_SIZE];
    }

}