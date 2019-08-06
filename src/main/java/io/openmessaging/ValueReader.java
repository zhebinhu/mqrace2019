package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_PAGE_SIZE);

    /**
     * 消息总数
     */
    private int messageNum = 0;

    /**
     * 缓存中最大消息
     */
    private long bufferMaxIndex = -1L;

    /**
     * 缓存中最小消息
     */
    private long bufferMinIndex = -1L;

    private volatile boolean inited = false;

    private List<ValueTag> valueTagList = new ArrayList<>();

    ThreadLocal<ValuePage> valuePage = new ThreadLocal<>();

    ThreadLocal<Integer> pageIndex = new ThreadLocal<>();

    ThreadLocal<Integer> tag = new ThreadLocal<>();

    ThreadLocal<Integer> offsetA = new ThreadLocal<>();

    ThreadLocal<Integer> offsetB = new ThreadLocal<>();

    int i = 0;

    int k = 0;

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
        int value = (int) (message.getA() - message.getT());
        if (!buffer.hasRemaining()) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.clear();
        }

        if (valuePage.get() == null) {
            valuePage.set(new ValuePage());
        }
        if (tag.get() == null) {
            tag.set(-1);
        }

        if (tag.get() == -1 || value - tag.get() > Byte.MAX_VALUE || value - tag.get() < Byte.MIN_VALUE) {
            tag.set(value);
            valueTagList.add(new ValueTag(tag.get(), messageNum));
        }

        if (i == Constants.VALUE_PAGE_SIZE) {
            pageCache.put(k, valuePage.get());
            k++;
            valuePage.set(new ValuePage());
            i = 0;
        }
        if (k < Constants.VALUE_CACHE_SIZE / Constants.VALUE_PAGE_SIZE) {
            valuePage.get().byteBuffer.put(i,(byte) (value - tag.get()));
            i++;
        }
        buffer.put((byte) (value - tag.get()));

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

    public int getValue(int offset) {

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
        if (offsetA.get() == null) {
            offsetA.set(-1);
        }
        if (offsetB.get() == null) {
            offsetB.set(-1);
        }
        if (offset < offsetA.get() || offset >= offsetB.get()) {
            int thisIndex = Collections.binarySearch(valueTagList, new ValueTag(0, offset));
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            tag.set(valueTagList.get(thisIndex).getValue());
            offsetA.set(valueTagList.get(thisIndex).getOffset());
            if (thisIndex < valueTagList.size() - 1) {
                offsetB.set(valueTagList.get(thisIndex + 1).getOffset());
            } else {
                offsetB.set(messageNum);
            }
        }

        if (pageIndex.get() == offset / Constants.VALUE_PAGE_SIZE) {
            return tag.get() + valuePage.get().byteBuffer.get(offset % Constants.VALUE_PAGE_SIZE);
        }

        pageIndex.set(offset / Constants.VALUE_PAGE_SIZE);

        valuePage.set(pageCache.get(pageIndex.get()));

        if (valuePage.get() == null) {

            valuePage.set(new ValuePage());
            try {
                valuePage.get().byteBuffer.clear();
                fileChannel.read(valuePage.get().byteBuffer, pageIndex.get() * Constants.VALUE_PAGE_SIZE);
                valuePage.get().byteBuffer.flip();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            pageCache.put(pageIndex.get(), valuePage.get());
        }

        return tag.get() + valuePage.get().byteBuffer.get(offset % Constants.VALUE_PAGE_SIZE);

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
    }

}