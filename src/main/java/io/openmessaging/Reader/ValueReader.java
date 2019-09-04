package io.openmessaging.Reader;

import io.openmessaging.*;
import io.openmessaging.Context.ValueContext;

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

    private FileChannel fileChannel;

    private final int bufNum = 2;

    private ByteBuffer[] buffers = new ByteBuffer[bufNum];

    private Future[] futures = new Future[bufNum];

    private ByteBuffer cache = ByteBuffer.allocateDirect(120000000 * 2);

    private short[] cache2;

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

    //private byte[] cache;

    private byte len = 0;

    private ValueTags valueTags = new ValueTags(100);

    private long real = 0;

    //private long base;

    public ValueReader(int num) {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + num + ".value", "rw").getChannel();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_CAP);
        }
    }

    public void put(Message message) {
        long value = message.getA();
        if (messageNum >= 50000000 && messageNum < 170000000) {
            cache.putShort((short) value);
            value = value >>> 16;
        }
        else if (messageNum >= 170000000 && messageNum < 210000000) {
            if (cache2 == null) {
                cache2 = new short[40000000];
            }
            cache2[messageNum - 170000000] = (short) value;
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
        for (int i = size - 1; i >= 0; i--) {
            short s = (short) ((value) >>> (i << 4));
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
        System.out.println("valueTags size:" + valueTags.size());
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
        if (index >= 50000000 && index < 170000000) {
            value = (value << 16) | (cache.getShort((index - 50000000) * 2) & 0xffff);
        }
        else if (index >= 170000000 && index < 210000000) {
            value = (value << 16) | (cache2[index - 170000000] & 0xffff);
        }
        return value;
    }

    public Avg avg(int offsetA, int offsetB, long aMin, long aMax, ValueContext valueContext) {
        Avg avg = new Avg();
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
            if (offsetA >= 50000000 && offsetA < 170000000) {
                value = (value << 16) | (cache.getShort((offsetA - 50000000) * 2) & 0xffff);
            }
            else if (offsetA >= 170000000 && offsetA < 210000000) {
                value = (value << 16) | (cache2[offsetA - 170000000] & 0xffff);
            }
            if (value <= aMax && value >= aMin) {
                avg.sum += value;
                avg.count++;
            }
            offsetA++;
        }
        return avg;
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
//        long f = 0xff00000000000000L;
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