package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;

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
     * 文件通道
     */
    private static FileChannel fileChannel;

    /**
     * 堆外内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_NUM);

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;

    static {
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + "100.value", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
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
        buffer.putLong(message.getA());
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

    public long get(int index, ValueContext valueContext) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
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
        return valueContext.buffer.getLong();
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, ValueContext valueContext) {
        long sum = 0;
        int count = 0;
        int bufferCount = 0;
        int start = offsetA;
        long value =0;
        if (offsetA >= valueContext.bufferMinIndex && offsetA < valueContext.bufferMaxIndex) {
            valueContext.buffer.position((offsetA - valueContext.bufferMinIndex) * Constants.VALUE_SIZE);
        } else {
            //找到合适的buffer
            updateContext(offsetA, offsetB, valueContext);
            valueContext.buffer.clear();
            try {
                fileChannel.read(valueContext.buffer, ((long) offsetA) * Constants.VALUE_SIZE);
                bufferCount++;
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            valueContext.buffer.flip();
        }
        while (offsetA < offsetB) {
            if (offsetA >= valueContext.bufferMaxIndex) {
                updateContext(offsetA, offsetB, valueContext);
                valueContext.buffer.clear();
                try {
                    fileChannel.read(valueContext.buffer, ((long) offsetA) * Constants.VALUE_SIZE);
                    bufferCount++;
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
                valueContext.buffer.flip();
            }
            try {
                value = valueContext.buffer.getLong();
            }catch (Exception e){
                System.out.println("e");
            }

            if (value <= aMax && value >= aMin) {
                sum += value;
                count++;
            }
            offsetA++;
        }
        System.out.println("num:" + (offsetB - start) + " buffer count:" + bufferCount);
        return count == 0 ? 0 : sum / count;
    }

    private void updateContext(int offsetA, int offsetB, ValueContext valueContext) {
        int i = 0;
        while ((1 << (i + 1)) <= (offsetB - offsetA) / Constants.VALUE_NUM) {
            i = i + 1;
        }
        valueContext.buffer = valueContext.bufferList.get(i);
        valueContext.bufferMinIndex = offsetA;
        valueContext.bufferMaxIndex = Math.min(offsetA + (Constants.VALUE_NUM << i), messageNum);
    }
}