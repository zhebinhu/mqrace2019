package io.openmessaging.Reader;

import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.HalfByte;
import io.openmessaging.Message;
import io.openmessaging.MessagePool;
import io.openmessaging.TimeTags;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {

    int cap = 100000000;

    private byte[] cache = new byte[100000000];

    private TimeTags timeTags = new TimeTags(5000000);

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private long tag = 0;

    private ValueReader valueReader;

    private DataReader dataReader;

    public TimeReader(ValueReader valueReader, DataReader dataReader) {
        this.valueReader = valueReader;
        this.dataReader = dataReader;
    }

    public void put(Message message) {
        long t = message.getT();

        if (tag == 0 || t > tag + 15) {
            tag = t;
            timeTags.add(t, msgNum);
        }

        if (msgNum % 2 == 0) {
            halfByte.setRight((byte) (t - tag));
        } else {
            halfByte.setLeft((byte) (t - tag));
            if (msgNum / 2 >= cap) {
                cap = cap + 20000000;
                cache = Arrays.copyOf(cache, cap);
            }
            cache[msgNum / 2] = halfByte.getByte();
            halfByte.setByte((byte) 0);
        }
        msgNum++;
    }

    public void init() {
        //cache.put(msgNum / 2, halfByte.getByte());
        if (msgNum / 2 >= cap) {
            cap = cap + 20000000;
            cache = Arrays.copyOf(cache, cap);
        }
        cache[msgNum / 2] = halfByte.getByte();
        //System.out.println("time max:" + max + " count256:" + count256 + " count65536:" + count65536 + " count15:" + count15);
        System.out.println("msgnum:" + msgNum);
        init = true;
    }

    public int getOffset(long time) {
        if (!init) {
            synchronized (this) {
                if (!init) {
                    init();
                }
            }
        }
        int tagIndex = timeTags.tagIndex(time);
        long pTag = timeTags.getTag(tagIndex);
        int pOffset = timeTags.getOffset(tagIndex);
        long pTime = pTag;
        if (pOffset > msgNum) {
            System.out.println("e");
        }
        while (pTime < time && pOffset < msgNum) {
            pOffset++;
            if (pOffset % 2 == 0) {
                pTime = pTag + HalfByte.getRight(cache[pOffset / 2]);
            } else {
                pTime = pTag + HalfByte.getLeft(cache[pOffset / 2]);
            }
        }
        return pOffset;
    }

    public long get(int offset, TimeContext context) {
        if (offset < context.offsetA || offset >= context.offsetB) {
            int tagIndex = timeTags.offsetIndex(offset);
            context.tag = timeTags.getTag(tagIndex);
            context.offsetA = timeTags.getOffset(tagIndex);
            if (tagIndex == timeTags.size() - 1) {
                context.offsetB = msgNum;
            } else {
                context.offsetB = timeTags.getOffset(tagIndex + 1);
            }
        }
        if (offset % 2 == 0) {
            return context.tag + HalfByte.getRight(cache[offset / 2]);
        } else {
            return context.tag + HalfByte.getLeft(cache[offset / 2]);
        }
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool, TimeContext timeContext, ValueContext valueContext, DataContext dataContext) {
        List<Message> result = new ArrayList<>();
        int offsetA = getOffset(tMin);
        if (offsetA > msgNum) {
            System.out.println("e");
        }
        if (offsetA == msgNum) {
            return result;
        }
        int tagIndex = timeTags.offsetIndex(offsetA);
        timeContext.tag = timeTags.getTag(tagIndex);
        timeContext.offsetA = timeTags.getOffset(tagIndex);
        if (tagIndex == timeTags.size() - 1) {
            timeContext.offsetB = msgNum;
        } else {
            timeContext.offsetB = timeTags.getOffset(tagIndex + 1);
        }
        while (offsetA < msgNum) {
            long time;
            if (offsetA < timeContext.offsetA || offsetA >= timeContext.offsetB) {
                tagIndex++;
                timeContext.tag = timeTags.getTag(tagIndex);
                timeContext.offsetA = timeTags.getOffset(tagIndex);
                if (tagIndex == timeTags.size() - 1) {
                    timeContext.offsetB = msgNum;
                } else {
                    timeContext.offsetB = timeTags.getOffset(tagIndex + 1);
                }
            }
            if (offsetA % 2 == 0) {
                time = timeContext.tag + HalfByte.getRight(cache[offsetA / 2]);
            } else {
                time = timeContext.tag + HalfByte.getLeft(cache[offsetA / 2]);
            }
            if (time > tMax) {
                break;
            }
            long value = valueReader.get(offsetA, valueContext);
            if (value < aMin || value > aMax) {
                offsetA++;
                continue;
            }
            Message message = messagePool.get();
            message.setT(time);
            message.setA(value);
            dataReader.getData(offsetA, message, dataContext);
            result.add(message);
            offsetA++;
        }
        return result;
    }
}
