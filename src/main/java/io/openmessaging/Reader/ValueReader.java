package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;

import java.io.FileNotFoundException;
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

    private final int bufNum = 8;

    /**
     * 堆外内存
     */
    private ByteBuffer[] buffers = new ByteBuffer[bufNum];

    private Future[] futures = new Future[bufNum];

    private int index = 0;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;

    public ValueReader() {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.value", "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_CAP);
        }

    }

    public void put(Message message) {
        long value = message.getA();
        if (!buffers[index].hasRemaining()) {
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
        buffers[index].putShort((short) (value >>> 32));
        buffers[index].putInt((int) value);
        messageNum++;
    }

    public void init() {
        try {
            for (Future future : futures) {
                if (!future.isDone()) {
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
    }

    public long get(int index, ValueContext valueContext) {
        if (index >= valueContext.bufferMinIndex && index < valueContext.bufferMaxIndex) {
            valueContext.buffer.position((index - valueContext.bufferMinIndex) * Constants.VALUE_SIZE);
        } else {
            valueContext.buffer.clear();
            try {
                fileChannel.read(valueContext.buffer, ((long) index) * Constants.VALUE_SIZE);
                valueContext.bufferMinIndex = index;
                valueContext.bufferMaxIndex = Math.min(index + Constants.VALUE_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            valueContext.buffer.flip();
        }
        long a = valueContext.buffer.getShort() & 0x000000000000ffffL;
        long b = valueContext.buffer.getInt() & 0x00000000ffffffffL;
        return a << 32 | b;
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, ValueContext valueContext) {
        long sum = 0;
        int count = 0;
        long value;
        //找到合适的buffer
        updateContext(offsetA, offsetB, valueContext);
        while (offsetA < offsetB) {
            long a = valueContext.buffer.getShort() & 0x000000000000ffffL;
            long b = valueContext.buffer.getInt() & 0x00000000ffffffffL;
            value = a << 32 | b;
            if (value <= aMax && value >= aMin) {
                sum += value;
                count++;
            }
            offsetA++;
        }
        return count == 0 ? 0 : sum / count;
    }

    private void updateContext(int offsetA, int offsetB, ValueContext valueContext) {
        int i = (offsetB - offsetA) * 3 >>> 11;
        valueContext.buffer = valueContext.bufferList.get(i);
        valueContext.buffer.clear();
        try {
            fileChannel.read(valueContext.buffer, ((long) offsetA) * Constants.VALUE_SIZE);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }
        valueContext.buffer.flip();
    }
}