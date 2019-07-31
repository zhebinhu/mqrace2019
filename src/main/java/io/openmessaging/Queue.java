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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    /**
     * 消息总数
     */
    private long messageNum = 0L;

    /**
     * 最大时间
     */
    private long maxTime = -1L;

    /**
     * 缓存中最大消息
     */
    private long bufferMaxIndex = -1L;

    /**
     * 缓存中最小消息
     */
    private long bufferMinIndex = -1L;

    private NavigableMap<Long, Long> indexMap = new TreeMap<Long, Long>();

    private volatile boolean inited = false;

    private DataReader dataReader;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public Queue(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".msg", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
        dataReader = new DataReader(num);
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
        dataReader.put(message);

        if (message.getT() / Constants.INDEX_RATE > maxTime) {
            maxTime = message.getT() / Constants.INDEX_RATE;
            indexMap.put(maxTime, messageNum);
        }
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

        if (tMin / Constants.INDEX_RATE > maxTime) {
            return result;
        } else {
            offsetA = indexMap.get(indexMap.higherKey(tMin / Constants.INDEX_RATE - 1));
        }
        if (tMax / Constants.INDEX_RATE >= maxTime) {
            offsetB = messageNum;
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
                        continue;
                    }
                    Message message = messagePool.get();
                    dataReader.getData(offsetA + i, message);
                    message.setT(time);
                    message.setA(value);
                    result.add(message);
                }
                offsetA += offset;

            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }
        return result;

    }

    public synchronized Avg getAvg(long aMin, long aMax, long tMin, long tMax) {
        Avg result = new Avg();

        if (indexMap.isEmpty()) {
            result.setTotal(0);
            result.setCount(0);
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

        if (tMin / Constants.INDEX_RATE > maxTime) {
            return result;
        } else {
            offsetA = indexMap.get(indexMap.higherKey(tMin / Constants.INDEX_RATE - 1));
        }
        if (tMax / Constants.INDEX_RATE >= maxTime) {
            offsetB = messageNum;
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
                        buffer.position(buffer.position()+Constants.MESSAGE_SIZE-8);
                        continue;
                    }
                    long value = buffer.getLong();
                    if (value < aMin || value > aMax) {
                        continue;
                    }
                    result.setCount(result.getCount()+1);
                    result.setTotal(result.getTotal()+value);
                }
                offsetA += offset;

            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }
        return result;
    }

}
