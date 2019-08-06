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
    private List<TimeTag> timeTagList = new ArrayList<>();

    private DataReader dataReader;

    private ValueReader valueReader;

    private TimeReader timeReader;

    /**
     * 合并相关参数
     */
    private int mergeOffset = 0;

    private long mergeTimeTag = -1;

    private int mergeOffsetB;

    private int mergeTagIndex = -1;

    public Queue(int num) {
        this.num = num;
        dataReader = new DataReader(num);
        valueReader = new ValueReader(num);
        timeReader = new TimeReader(num);
    }

    public void put(Message message) {
        if (maxTime == -1 || message.getT() > maxTime + 255) {
            maxTime = message.getT();
            timeTagList.add(new TimeTag(maxTime, messageNum));
        }

        timeReader.put((byte) (message.getT() - maxTime));
        valueReader.put(message);
        dataReader.put(message);

        messageNum++;
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {

        List<Message> result = new ArrayList<>();

        if (timeTagList.isEmpty()) {
            return result;
        }

        int offsetA;
        int offsetB;
        // 一级索引
        int thisIndex = 0;
        if (tMin > maxTime + 255) {
            return result;
        }
        if (tMin <= timeTagList.get(0).getTime()) {
            offsetA = 0;
        } else {
            thisIndex = Collections.binarySearch(timeTagList, new TimeTag(tMin,0));
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            offsetA = timeTagList.get(thisIndex).getOffset();
        }
        if (thisIndex == timeTagList.size() - 1) {
            offsetB = messageNum;
        } else {
            offsetB = timeTagList.get(thisIndex + 1).getOffset();
        }
        long baseTime = timeTagList.get(thisIndex).getTime();
        while (true) {
            if (offsetA == messageNum) {
                break;
            }
            if (offsetA == offsetB) {
                thisIndex++;
                if (thisIndex == timeTagList.size() - 1) {
                    offsetB = messageNum;
                } else {
                    offsetB = timeTagList.get(thisIndex + 1).getOffset();
                }
                baseTime = timeTagList.get(thisIndex).getTime();
            }
            long time = baseTime + (timeReader.getTime(offsetA) + 256) % 256;
            if (time < tMin) {
                offsetA++;
                continue;
            }
            if (time > tMax) {
                break;
            }
            long value = time + valueReader.getValue(offsetA);
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

    public Avg getAvg(long aMin, long aMax, long tMin, long tMax) {
        Avg result = new Avg();

        if (timeTagList.isEmpty()) {
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
        if (tMin <= timeTagList.get(0).getTime()) {
            offsetA = 0;
        } else {
            thisIndex = Collections.binarySearch(timeTagList, new TimeTag(tMin,0));
            if (thisIndex < 0) {
                thisIndex = Math.max(0, -(thisIndex + 2));
            }
            offsetA = timeTagList.get(thisIndex).getOffset();
        }
        if (thisIndex == timeTagList.size() - 1) {
            offsetB = messageNum;
        } else {
            offsetB = timeTagList.get(thisIndex + 1).getOffset();
        }
        long baseTime = timeTagList.get(thisIndex).getTime();
        while (true) {
            if (offsetA == messageNum) {
                break;
            }
            if (offsetA == offsetB) {
                thisIndex++;
                if (thisIndex == timeTagList.size() - 1) {
                    offsetB = messageNum;
                } else {
                    offsetB = timeTagList.get(thisIndex + 1).getOffset();
                }
                baseTime = timeTagList.get(thisIndex).getTime();
            }
            long time = baseTime + (timeReader.getTime(offsetA) + 256) % 256;
            if (time < tMin) {
                offsetA++;
                continue;
            }
            if (time > tMax) {
                break;
            }
            long value = time + valueReader.getValue(offsetA);
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

    /**
     * 用于合并
     *
     * @return
     */
    public TA get() {
        if (mergeOffset == messageNum) {
            return null;
        }
        if (mergeTimeTag == -1 || mergeOffset == mergeOffsetB) {
            mergeTagIndex++;
            mergeTimeTag = timeTagList.get(mergeTagIndex).getTime();
            if (mergeTagIndex + 1 == timeTagList.size()) {
                mergeOffsetB = messageNum;
            } else {
                mergeOffsetB = timeTagList.get(mergeTagIndex + 1).getOffset();
            }
        }
        long time = mergeTimeTag + timeReader.getTime(mergeOffset);
        long value = time + valueReader.getValue(mergeOffset);
        mergeOffset++;
        return new TA(time, value);
    }

}
