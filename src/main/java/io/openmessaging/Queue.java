package io.openmessaging;

import java.util.*;

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
    private List<TimeTag2> timeTag2List = new ArrayList<>();

    private DataReader2 dataReader2;

    private ValueReader2 valueReader2;

    private TimeReader2 timeReader2;

    /**
     * 合并相关参数
     */
    private int mergeOffset = 0;

    private long mergeTimeTag = -1;

    private int mergeOffsetB;

    private int mergeTagIndex = -1;

    public Queue(int num) {
        this.num = num;
        dataReader2 = new DataReader2(num);
        valueReader2 = new ValueReader2(num);
        timeReader2 = new TimeReader2(num);
    }

    public void put(Message message) {
        if (maxTime == -1 || message.getT() > maxTime + 255) {
            maxTime = message.getT();
            timeTag2List.add(new TimeTag2(maxTime, messageNum));
        }

        timeReader2.put((byte) (message.getT() - maxTime));
        valueReader2.put(message);
        dataReader2.put(message);

        messageNum++;
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {

        List<Message> result = new ArrayList<>();

        if (timeTag2List.isEmpty()) {
            return result;
        }

        int offsetA;
        int offsetB;
        // 一级索引
        int thisIndex = 0;
        if (tMin > maxTime + 255) {
            return result;
        }
        if (tMin <= timeTag2List.get(0).getTime()) {
            offsetA = 0;
        } else {
            thisIndex = Collections.binarySearch(timeTag2List, new TimeTag2(tMin, 0));
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            offsetA = timeTag2List.get(thisIndex).getOffset();
        }
        if (thisIndex == timeTag2List.size() - 1) {
            offsetB = messageNum;
        } else {
            offsetB = timeTag2List.get(thisIndex + 1).getOffset();
        }
        long baseTime = timeTag2List.get(thisIndex).getTime();
        while (true) {
            if (offsetA == messageNum) {
                break;
            }
            if (offsetA == offsetB) {
                thisIndex++;
                if (thisIndex == timeTag2List.size() - 1) {
                    offsetB = messageNum;
                } else {
                    offsetB = timeTag2List.get(thisIndex + 1).getOffset();
                }
                baseTime = timeTag2List.get(thisIndex).getTime();
            }
            long time = baseTime + (timeReader2.getTime(offsetA) + 256) % 256;
            if (time < tMin) {
                offsetA++;
                continue;
            }
            if (time > tMax) {
                break;
            }
            long value = time + valueReader2.getValue(offsetA);
            if (value < aMin || value > aMax) {
                offsetA++;
                continue;
            }
            Message message = messagePool.get();
            dataReader2.getData(offsetA, message);
            message.setT(time);
            message.setA(value);
            result.add(message);
            offsetA++;
        }
        return result;

    }

    public Avg getAvg(long aMin, long aMax, long tMin, long tMax) {
        Avg result = new Avg();

        if (timeTag2List.isEmpty()) {
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
        if (tMin <= timeTag2List.get(0).getTime()) {
            offsetA = 0;
        } else {
            thisIndex = Collections.binarySearch(timeTag2List, new TimeTag2(tMin, 0));
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            offsetA = timeTag2List.get(thisIndex).getOffset();
        }
        if (thisIndex == timeTag2List.size() - 1) {
            offsetB = messageNum;
        } else {
            offsetB = timeTag2List.get(thisIndex + 1).getOffset();
        }
        long baseTime = timeTag2List.get(thisIndex).getTime();
        while (true) {
            if (offsetA == messageNum) {
                break;
            }
            if (offsetA == offsetB) {
                thisIndex++;
                if (thisIndex == timeTag2List.size() - 1) {
                    offsetB = messageNum;
                } else {
                    offsetB = timeTag2List.get(thisIndex + 1).getOffset();
                }
                baseTime = timeTag2List.get(thisIndex).getTime();
            }
            long time = baseTime + (timeReader2.getTime(offsetA) + 256) % 256;
            if (time < tMin) {
                offsetA++;
                continue;
            }
            if (time > tMax) {
                break;
            }
            long value = time + valueReader2.getValue(offsetA);
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
