package io.openmessaging.Reader;

import io.openmessaging.Context.TimeContext;
import io.openmessaging.HalfByte;
import io.openmessaging.Message;
import io.openmessaging.TimeTags;
import io.openmessaging.UnsafeWrapper;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {

    //private byte[] cache = new byte[Integer.MAX_VALUE / 2];
    private long base;

    private TimeTags timeTags = new TimeTags(100000000);

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private long tag = 0;

    private long max = 0;

    TimeReader() {
        base = UnsafeWrapper.unsafe.allocateMemory(Integer.MAX_VALUE / 2);
    }

    public void put(Message message) {
        long t = message.getT();
        if(t>max){
            max=t;
        }
        if (tag == 0 || t > tag + 15) {
            tag = t;
            timeTags.add(t, msgNum);
        }

        if (msgNum % 2 == 0) {
            halfByte.setRight((byte) (t - tag));
        } else {
            halfByte.setLeft((byte) (t - tag));
            UnsafeWrapper.unsafe.putByte(base + msgNum / 2, halfByte.getByte());
            halfByte.setByte((byte) 0);
        }
        msgNum++;
    }

    public void init() {
        UnsafeWrapper.unsafe.putByte(base + msgNum / 2, halfByte.getByte());
        System.out.println("max:" + max);
    }

    public int getOffset(long time) {
        int tagIndex = timeTags.tagIndex(time);
        long pTag = timeTags.getTag(tagIndex);
        int pOffset = timeTags.getOffset(tagIndex);
        long pTime = pTag;
        while (pTime < time && pOffset < msgNum) {
            pOffset++;
            if (pOffset % 2 == 0) {
                pTime = pTag + HalfByte.getRight(UnsafeWrapper.unsafe.getByte(base + pOffset / 2));
            } else {
                pTime = pTag + HalfByte.getLeft(UnsafeWrapper.unsafe.getByte(base + pOffset / 2));
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
            return context.tag + HalfByte.getRight(UnsafeWrapper.unsafe.getByte(base+offset / 2));
        } else {
            return context.tag + HalfByte.getLeft(UnsafeWrapper.unsafe.getByte(base+offset / 2));
        }
    }
}
