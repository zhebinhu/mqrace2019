package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;
import io.openmessaging.ValueTags;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class ValueReader {

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    private final int bufNum = 2;

    /**
     * 堆外内存
     */
    private ByteBuffer[] buffers = new ByteBuffer[bufNum];

    private Future[] futures = new Future[bufNum];

    private int index = 0;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        return thread;
    });

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private short[] cache;

    private byte len = 0;

    private ValueTags valueTags = new ValueTags(50000);

    private long real = 0;

    private ByteBuffer cache2 = ByteBuffer.allocateDirect(1024 * 1024 * 1024);

    public ValueReader() {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.value", "rw").getChannel();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_CAP);
        }
        cache = new short[Integer.MAX_VALUE / 2];
    }

    public void put(Message message) {
        long value = message.getA();
        if (messageNum > 256 * 1024 * 1024 && messageNum <= 1280 * 1024 * 1024) {
            cache[messageNum] = (short) value;
            value = value >>> 16;
        } else if (messageNum > 256 * 1024 * 1024 && messageNum <= 1792 * 1024 * 1024) {
            cache2.putShort((short) value);
            value = value >>> 16;
        }
        byte size = getShortSize(value);
        if (size != len) {
            len = size;
            valueTags.add(real, messageNum, size);
        }
        real += size * 2;
        if (buffers[index].remaining() < size * 2) {
            ByteBuffer tmpBuffer = buffers[index];
            int newIndex = (index + 1) % bufNum;
            tmpBuffer.flip();
            try {
                if (futures[index] == null) {
                    futures[index] = executorService.submit(() -> fileChannel.write(tmpBuffer));
                } else {
                    if (!futures[newIndex].isDone()) {
                        System.out.println("value block");
                        futures[newIndex].get();
                    }
                    futures[index] = executorService.submit(() -> fileChannel.write(tmpBuffer));
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            index = newIndex;
            buffers[index].clear();
        }
        for (int i = 1; i <= size; i++) {
            short s = (short) ((value) >>> ((size - i) << 4));
            buffers[index].putShort(s);
        }
        messageNum++;
    }

    public void init() {
        try {
            for (Future future : futures) {
                if (future != null && !future.isDone()) {
                    future.get();
                }
            }
            if (buffers[index].hasRemaining()) {
                buffers[index].flip();
                fileChannel.write(buffers[index]);
                buffers[index].clear();
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        System.out.println("valueTags:" + valueTags.size());
    }

    public long get(int index, ValueContext valueContext) {
        if (index == valueContext.nextOffset) {
            valueTags.update(valueContext);
        }
        byte tag = valueContext.tag;
        long value = 0;
        for (int i = 0; i < tag; i++) {
            value = (value << 16) | (valueContext.buffer.getShort() & 0xffff);
        }
        if (index > 256 * 1024 * 1024 && index <= 1280 * 1024 * 1024) {
            value = value << 16 | (cache[index] & 0xffff);
        } else if (index > 256 * 1024 * 1024 && index <= 1792 * 1024 * 1024) {
            value = value << 16 | (cache2.getShort(index * 16) & 0xffff);
        }
        return value;
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, ValueContext valueContext) {
        long sum = 0;
        int count = 0;
        //找到合适的buffer
        updateContext(offsetA, offsetB, valueContext);
        while (offsetA < offsetB) {
            if (offsetA == valueContext.nextOffset) {
                valueTags.update(valueContext);
            }
            byte tag = valueContext.tag;
            long value = 0;
            for (int i = 0; i < tag; i++) {
                value = (value << 16) | (valueContext.buffer.getShort() & 0xffff);
            }
            if (offsetA > 256 * 1024 * 1024 && offsetA <= 1280 * 1024 * 1024) {
                value = value << 16 | (cache[offsetA] & 0xffff);
            } else if (offsetA > 256 * 1024 * 1024 && offsetA <= 1792 * 1024 * 1024) {
                value = value << 16 | (cache2.getShort(offsetA * 16) & 0xffff);
            }
            if (value <= aMax && value >= aMin) {
                sum += value;
                count++;
            }
            offsetA++;
        }
        return count == 0 ? 0 : sum / count;
    }

    public void updateContext(int offsetA, int offsetB, ValueContext valueContext) {
        long realA = valueTags.getRealOffset(offsetA, valueContext);
        long realB = valueTags.getRealOffset(offsetB);
        valueContext.buffer.clear();
        valueContext.buffer.limit((int) (realB - realA));
        try {
            fileChannel.read(valueContext.buffer, realA);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }
        valueContext.buffer.flip();
    }

    //    private byte getByteSize(long value) {
    //        long f = 0xff000000000000L;
    //        for (byte i = Constants.VALUE_SIZE; i >= 0; i--) {
    //            if ((value & f) != 0) {
    //                return i;
    //            }
    //            f = f >>> 8;
    //        }
    //        return 0;
    //    }

    private byte getShortSize(long value) {
        long f = 0xffff000000000000L;
        for (byte i = Constants.VALUE_SIZE / 2; i >= 0; i--) {
            if ((value & f) != 0) {
                return i;
            }
            f = f >>> 16;
        }
        return 0;
    }
}