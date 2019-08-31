package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;
import io.openmessaging.UnsafeWrapper;
import io.openmessaging.ValueTags;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
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

    private FileChannel fileChannel2;

    private final int bufNum = 2;

    private final int bufNum2 = 2;

    /**
     * 堆外内存
     */
    private ByteBuffer[] buffers = new ByteBuffer[bufNum];

    private Future[] futures = new Future[bufNum];

    private ByteBuffer[] buffers2 = new ByteBuffer[bufNum2];

    private Future[] futures2 = new Future[bufNum2];

    private int index = 0;

    private int index2 = 0;

    private MappedByteBuffer mappedByteBuffer;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        return thread;
    });

    private ExecutorService executorService2 = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        return thread;
    });

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private byte[] cache;

    private byte len = 0;

    private ValueTags valueTags = new ValueTags(16000000);

    private long real = 0;

    private long base;

    public ValueReader() {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.value", "rw").getChannel();
            fileChannel2 = new RandomAccessFile(Constants.URL + "101.value", "rw").getChannel();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_CAP);
        }
        for (int i = 0; i < bufNum2; i++) {
            buffers2[i] = ByteBuffer.allocateDirect(Constants.VALUE_CAP);
        }
        cache = new byte[Integer.MAX_VALUE - 2];
        base = UnsafeWrapper.unsafe.allocateMemory(1000000000);
        UnsafeWrapper.unsafe.setMemory(base, 1000000000, (byte) 0);
    }

    public void put(Message message) {
        long value = message.getA();
        byte value2 = (byte) value;
        if (!buffers2[index2].hasRemaining()) {
            ByteBuffer tmpBuffer = buffers2[index2];
            int newIndex = (index2 + 1) % bufNum2;
            tmpBuffer.flip();
            try {
                if (futures2[index2] == null) {
                    futures2[index2] = executorService2.submit(() -> fileChannel2.write(tmpBuffer));
                } else {
                    if (!futures2[newIndex].isDone()) {
                        System.out.println("value2 block");
                        futures2[newIndex].get();
                    }
                    futures2[index2] = executorService2.submit(() -> fileChannel2.write(tmpBuffer));
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            index2 = newIndex;
            buffers2[index2].clear();
        }
        buffers2[index2].put(value2);
        value = value >>> 8;
        if (messageNum >= 500000000 && messageNum < 1500000000) {
            UnsafeWrapper.unsafe.putByte(base + messageNum - 500000000, (byte) value);
            value = value >>> 8;
        }
        cache[messageNum] = (byte) value;
        value = value >>> 8;
        byte size = getByteSize(value);
        if (size != len) {
            len = size;
            valueTags.add(real, messageNum, size);
        }
        real += size;
        if (buffers[index].remaining() < size) {
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
            byte b = (byte) ((value) >>> (i << 3));
            buffers[index].put(b);
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
            for (Future future : futures2) {
                if (future != null && !future.isDone()) {
                    future.get();
                }
            }
            if (buffers[index].hasRemaining()) {
                buffers[index].flip();
                fileChannel.write(buffers[index]);
                buffers[index].clear();
            }
            if (buffers2[index2].hasRemaining()) {
                buffers2[index2].flip();
                fileChannel2.write(buffers2[index2]);
                buffers2[index2].clear();
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        System.out.println("valuetags size:" + valueTags.size());
        try {
            mappedByteBuffer = fileChannel2.map(FileChannel.MapMode.READ_ONLY, 0L, (long) messageNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long get(int index, ValueContext valueContext) {
        if (index == valueContext.nextOffset) {
            valueTags.update(valueContext);
        }
        byte tag = valueContext.tag;
        long value = 0;
        for (int i = 0; i < tag; i++) {
            value = (value << 8) | (valueContext.buffer.get() & 0xff);
        }
        value = value << 8 | (cache[index] & 0xff);
        if (index >= 500000000 && index < 1500000000) {
            value = value << 8 | (UnsafeWrapper.unsafe.getByte(base + index - 500000000) & 0xff);
        }
        value = value << 8 | (mappedByteBuffer.get(index) & 0xff);
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
                value = (value << 8) | (valueContext.buffer.get() & 0xff);
            }
            value = value << 8 | (cache[offsetA] & 0xff);
            if (offsetA >= 500000000 && offsetA < 1500000000) {
                value = value << 8 | (UnsafeWrapper.unsafe.getByte(base + offsetA - 500000000) & 0xff);
            }
            value = value << 8 | (mappedByteBuffer.get(index) & 0xff);
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

    private byte getByteSize(long value) {
        long f = 0xff000000000000L;
        for (byte i = Constants.VALUE_SIZE; i >= 0; i--) {
            if ((value & f) != 0) {
                return i;
            }
            f = f >>> 8;
        }
        return 0;
    }
}