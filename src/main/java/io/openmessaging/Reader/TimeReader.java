package io.openmessaging.Reader;

import io.openmessaging.Context.TimeContext;
import io.openmessaging.HalfByte;
import io.openmessaging.Message;
import io.openmessaging.TimeTags;

import java.util.Arrays;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {
    private int cap = 100000000;

    private byte[] base = new byte[cap];

    private TimeTags timeTags = new TimeTags(10000000);

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private long tag = 0;

    public void put(Message message) {
        long t = message.getT();
        if (tag == 0 || t > tag + 255) {
            tag = t;
            timeTags.add(t, msgNum);
        }
        if (msgNum >= cap) {
            cap = cap + 100000000;
            base = Arrays.copyOf(base, cap);
        }
        base[msgNum] = (byte) (t - tag);
//        if (msgNum % 2 == 0) {
//            halfByte.setRight((byte) (t - tag));
//        } else {
//            halfByte.setLeft((byte) (t - tag));
//            base[msgNum / 2] = halfByte.getByte();
//            halfByte.setByte((byte) 0);
//        }
        msgNum++;
    }

    public void init() {
        //base[msgNum / 2] = halfByte.getByte();
        timeTags.add(tag + 256, msgNum);
        System.out.println("TimeTags size:" + timeTags.size());
    }

    public int getOffset(long time) {
        int tagIndex = timeTags.tagIndex(time);
        long pTag = timeTags.getTag(tagIndex);
        int pOffset = timeTags.getOffset(tagIndex);
        int pOffsetB = timeTags.getOffset(tagIndex + 1);
        long pTime = pTag;
        while (pTime < time && pOffset < pOffsetB) {
            pOffset++;
            pTime = pTag + (base[pOffset] & 0xff);
//            if (pOffset % 2 == 0) {
//                pTime = pTag + HalfByte.getRight(base[pOffset / 2]);
//            } else {
//                pTime = pTag + HalfByte.getLeft(base[pOffset / 2]);
//            }
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
        return context.tag + (base[offset] & 0xff);
//        if (offset % 2 == 0) {
//            return context.tag + HalfByte.getRight(base[offset / 2]);
//        } else {
//            return context.tag + HalfByte.getLeft(base[offset / 2]);
//        }
    }
}
