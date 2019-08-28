package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;
import io.openmessaging.UnsafeWrapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
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

    private final int bufNum = 8;

    private int[] valueTags = new int[8];

    private int valueLen = 0;

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

    private byte[] bytes = new byte[8];

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;

    private long base;

    public ValueReader() {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.value", "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_CAP);
        }
        for (int i = 0; i < 8; i++) {
            valueTags[i] = -1;
        }
        base = UnsafeWrapper.unsafe.allocateMemory(2 * 1024 * 1024 * 1024L);
    }

    public void put(Message message) {
        long value = message.getA();
        UnsafeWrapper.unsafe.putByte(base + 8 * messageNum, (byte) value);
        value = value >>> 8;
        valueLen = Math.max(getByteSize(value), valueLen);
        if (valueTags[valueLen - 1] == -1) {
            valueTags[valueLen - 1] = messageNum;
        }
        if (buffers[index].remaining() < valueLen) {
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
        Bits.putLong(bytes, 0, value);
        buffers[index].put(bytes, 8 - valueLen, valueLen);
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
        System.out.println(Arrays.toString(valueTags));
    }

    public long get(int index, ValueContext valueContext) {
        byte[] longs = new byte[8];
        long value = 0;
        int len;
        if (valueContext.msgLen > 0) {
            len = valueContext.msgLen;
        } else {
            len = getMsgLen(index);
        }
        for (int i = 0; i < len; i++) {
            value = (value << 8) | (valueContext.buffer.get() & 0xff);
        }
        value = value << 8 | (UnsafeWrapper.unsafe.getByte(base + index * 8) & 0xff);
        return value;
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, ValueContext valueContext) {
        long sum = 0;
        int count = 0;
        //找到合适的buffer
        updateContext(offsetA, offsetB, valueContext);
        while (offsetA < offsetB) {
            long value = 0;
            int len;
            if (valueContext.msgLen > 0) {
                len = valueContext.msgLen;
            } else {
                Arrays.fill(bytes, (byte) 0);
                len = getMsgLen(offsetA);
            }
            for (int i = 0; i < len; i++) {
                value = (value << 8) | (valueContext.buffer.get() & 0xff);
            }
            value = value << 8 | (UnsafeWrapper.unsafe.getByte(base + offsetA * 8) & 0xff);
            if (value <= aMax && value >= aMin) {
                sum += value;
                count++;
            }
            offsetA++;
        }
        return count == 0 ? 0 : sum / count;
    }

    public void updateContext(int offsetA, int offsetB, ValueContext valueContext) {
        long realA = getRealOffsetA(offsetA, valueContext);
        long realB = getRealOffsetB(offsetB, valueContext);
        if (valueContext.Alen == valueContext.Blen) {
            valueContext.msgLen = valueContext.Alen;
        }
        int i = (int) ((realB - realA) / Constants.PAGE_SIZE);
        if (i < 0) {
            System.out.println("e");
        }
        valueContext.buffer = valueContext.bufferList.get(i);
        valueContext.buffer.clear();
        try {
            fileChannel.read(valueContext.buffer, realA);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }
        valueContext.buffer.flip();
    }

    private int getByteSize(long value) {
        long f = 0xff00000000000000L;
        for (int i = 8; i >= 0; i--) {
            if ((value & f) != 0) {
                return i;
            }
            f = f >>> 8;
        }
        return 0;
    }

    public long getRealOffsetA(int offset, ValueContext valueContext) {
        long realOffset = 0;
        for (int i = 7; i >= 0; i--) {
            if (valueTags[i] == -1 || valueTags[i] > offset) {
                continue;
            }
            valueContext.Alen = i + 1;
            realOffset += ((long) offset - valueTags[i]) * (i + 1);
            offset = valueTags[i];
        }
        return realOffset;
    }

    public long getRealOffsetB(int offset, ValueContext valueContext) {
        long realOffset = 0;
        for (int i = 7; i >= 0; i--) {
            if (valueTags[i] == -1 || valueTags[i] > offset) {
                continue;
            }
            valueContext.Blen = i + 1;
            realOffset += ((long) offset - valueTags[i]) * (i + 1);
            offset = valueTags[i];
        }
        return realOffset;
    }

    public int getMsgLen(int index) {
        for (int i = 7; i >= 0; i--) {
            if (valueTags[i] == -1 || valueTags[i] > index) {
                continue;
            }
            return i + 1;
        }
        return 0;
    }
}