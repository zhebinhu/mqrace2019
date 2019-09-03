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
    private int cap = 200000000;

    private byte[] base = new byte[cap];

    private TimeTags timeTags = new TimeTags(5000000);

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
        msgNum++;
    }

    public void init() {
        System.out.println("TimeTags size:" + timeTags.size());
    }

    public int getOffset(long time) {
        int tagIndex = timeTags.tagIndex(time);
        long pTag = timeTags.getTag(tagIndex);
        int pOffset = timeTags.getOffset(tagIndex);
        int pOffsetB = timeTags.getOffset(tagIndex + 1);
        if (pOffsetB == 0) {
            pOffsetB = msgNum;
        }
        long pTime = pTag;
        while (pTime < time && pOffset < pOffsetB) {
            pOffset++;
            pTime = pTag + (base[pOffset] & 0xff);
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
    }
}
