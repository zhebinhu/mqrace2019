package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class Queue {
    /**
     * 队列的编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 直接内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    private Long index = 0L;

    private Long curTime = -1L;

    private NavigableMap<Long, Long> indexMap = new TreeMap<Long, Long>();

    private volatile boolean inited = false;

    public Queue(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.MESSAGE_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }
        buffer.putLong(message.getT());
        buffer.putLong(message.getA());
        buffer.put(message.getBody());
        if (message.getT() / Constants.INDEX_RATE > curTime) {
            curTime = message.getT() / Constants.INDEX_RATE;
            indexMap.put(curTime, index);
        }
        index++;
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

    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
        List<Message> result = new ArrayList<>();

        if (indexMap.isEmpty()) {
            return result;
        }

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }

        Long offsetA;
        Long offsetB;

        if (tMin / Constants.INDEX_RATE > curTime) {
            return result;
        } else {
            offsetA = indexMap.get(indexMap.higherKey(tMin / Constants.INDEX_RATE - 1));
        }
        if (tMax / Constants.INDEX_RATE >= curTime) {
            offsetB = index;
            System.out.println("at the end: "+offsetB);
        } else {
            offsetB = indexMap.get(indexMap.higherKey(tMax / Constants.INDEX_RATE));
        }

        while (offsetA < offsetB) {
            try {
                buffer.clear();
                fileChannel.read(buffer, offsetA * Constants.MESSAGE_SIZE);
                buffer.flip();
                long offset = Math.min(offsetB - offsetA, Constants.MESSAGE_NUM);
                for (int i = 0; i < offset; i++) {
                    long time = buffer.getLong();
                    if (time < tMin || time > tMax) {
                        buffer.position(buffer.position() + Constants.MESSAGE_SIZE - 8);
                        continue;
                    }
                    long value = buffer.getLong();
                    if (value < aMin || value > aMax) {
                        buffer.position(buffer.position() + Constants.MESSAGE_SIZE - 16);
                        continue;
                    }
                    Message message = messagePool.get();
                    buffer.get(message.getBody());
                    message.setT(time);
                    message.setA(value);
                    result.add(message);
                }
                offsetA += Constants.MESSAGE_NUM;

            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }
        return result;

    }

}
