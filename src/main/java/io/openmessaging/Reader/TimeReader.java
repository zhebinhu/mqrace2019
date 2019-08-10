package io.openmessaging.Reader;

import io.openmessaging.Context;
import io.openmessaging.HalfByte;
import io.openmessaging.Message;
import io.openmessaging.TimeTags;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {
    private long max = 0;

    private byte[] cache = new byte[Integer.MAX_VALUE / 2];

    private TimeTags timeTags = new TimeTags(70000000);

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private int tag = 0;

    public void put(Message message) {
        long t = message.getT();
        if (t > max) {
            max = t;
        }
        int time = (int) t;
        if (time > tag + 15) {
            tag = time;
            timeTags.add(time, msgNum);
        }
        if (msgNum % 2 == 0) {
            halfByte.setRight((byte) (time - tag));
        } else {
            halfByte.setLeft((byte) (time - tag));
            cache[msgNum / 2] = halfByte.getByte();
            halfByte.setByte((byte) 0);
        }
        msgNum++;
    }

    public void init() {
        cache[msgNum / 2] = halfByte.getByte();
        System.out.println("time max:" + max + " timeTags size:" + timeTags.size());
        init = true;
    }

    public int getOffset(int time) {
        if (!init) {
            synchronized (this) {
                if (!init) {
                    init();
                }
            }
        }
        int tagIndex = timeTags.tagIndex(time);
        int pTag = timeTags.getTag(tagIndex);
        int pOffset = timeTags.getOffset(tagIndex);
        int pTime = pTag;
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

    public int get(int offset, Context context) {
        if (!init) {
            synchronized (this) {
                if (!init) {
                    init();
                }
            }
        }

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
}
