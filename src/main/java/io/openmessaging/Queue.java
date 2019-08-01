package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
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
     * 消息总数
     */
    private int messageNum = 0;

    /**
     * 最大时间
     */
    private long maxTime = -1L;

    /**
     * 一级索引
     */
    private List<Index> indexList = new ArrayList<>();

    private DataReader dataReader;

    private ValueReader valueReader;

    private TimeReader timeReader;

    private Index index = new Index(0, 0);

    public Queue(int num) {
        this.num = num;
        dataReader = new DataReader(num);
        valueReader = new ValueReader(num);
        timeReader = new TimeReader(num);
    }

    public void put(Message message) {
        if (maxTime == -1 || message.getT() > maxTime + 255) {
            maxTime = message.getT();
            indexList.add(new Index(maxTime, messageNum));
        }

        timeReader.put((byte) (message.getT() - maxTime));
        valueReader.put(message);
        dataReader.put(message);

        messageNum++;
    }

    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
        List<Message> result = new ArrayList<>();

        if (indexList.isEmpty()) {
            return result;
        }

        int offsetA;
        int offsetB;
        // 一级索引
        int thisIndex = 0;

        if (tMin > maxTime + 255) {
            return result;
        }
        if (tMin <= indexList.get(0).getTime()) {
            offsetA = 0;
        } else {
            index.setTime(tMin);
            index.setOffset(0);
            thisIndex = Collections.binarySearch(indexList, index);
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            offsetA = indexList.get(thisIndex).getOffset();
        }
        if (thisIndex == indexList.size() - 1) {
            offsetB = messageNum;
        } else {
            offsetB = indexList.get(thisIndex + 1).getOffset();
        }
        long baseTime = indexList.get(thisIndex).getTime();
        while (true) {
            if (offsetA == messageNum) {
                break;
            }
            if (offsetA == offsetB) {
                thisIndex++;
                if (thisIndex == indexList.size() - 1) {
                    offsetB = messageNum;
                } else {
                    offsetB = indexList.get(thisIndex + 1).getOffset();
                }
                baseTime = indexList.get(thisIndex).getTime();
            }
            long time = baseTime + (timeReader.getTime(offsetA) + 256) % 256;
            if (time < tMin) {
                offsetA++;
                continue;
            }
            if (time > tMax) {
                break;
            }
            long value = valueReader.getValue(offsetA);
            if (value < aMin || value > aMax) {
                offsetA++;
                continue;
            }
            Message message = messagePool.get();
            dataReader.getData(offsetA, message);
            message.setT(time);
            message.setA(value);
            result.add(message);
            offsetA++;
        }
        return result;

    }

    public synchronized Avg getAvg(long aMin, long aMax, long tMin, long tMax) {
        Avg result = new Avg();

        if (indexList.isEmpty()) {
            result.setTotal(0);
            result.setCount(0);
            return result;
        }

        int offsetA;
        int offsetB;
        // 一级索引
        int thisIndex = 0;

        if (tMin > maxTime + 255) {
            return result;
        }
        if (tMin <= indexList.get(0).getTime()) {
            offsetA = 0;
        } else {
            index.setTime(tMin);
            index.setOffset(0);
            thisIndex = Collections.binarySearch(indexList, index);
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            offsetA = indexList.get(thisIndex).getOffset();
        }
        if (thisIndex == indexList.size() - 1) {
            offsetB = messageNum;
        } else {
            offsetB = indexList.get(thisIndex + 1).getOffset();
        }
        long baseTime = indexList.get(thisIndex).getTime();
        while (true) {
            if (offsetA == messageNum) {
                break;
            }
            if (offsetA == offsetB) {
                thisIndex++;
                if (thisIndex == indexList.size() - 1) {
                    offsetB = messageNum;
                } else {
                    offsetB = indexList.get(thisIndex + 1).getOffset();
                }
                baseTime = indexList.get(thisIndex).getTime();
            }
            long time = baseTime + (timeReader.getTime(offsetA) + 256) % 256;
            if (time < tMin) {
                offsetA++;
                continue;
            }
            if (time > tMax) {
                break;
            }
            long value = valueReader.getValue(offsetA);
            if (value < aMin || value > aMax) {
                offsetA++;
                continue;
            }
            result.setCount(result.getCount() + 1);
            result.setTotal(result.getTotal() + value);
            offsetA++;
        }
        return result;
    }

}
