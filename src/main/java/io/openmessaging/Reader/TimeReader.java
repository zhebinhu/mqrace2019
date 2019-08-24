package io.openmessaging.Reader;

import io.openmessaging.Context.TimeContext;
import io.openmessaging.HalfByte;
import io.openmessaging.Message;
import io.openmessaging.TimeTags;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {

    private byte[] cache = new byte[Integer.MAX_VALUE / 2];

    private TimeTags timeTags = new TimeTags(100000000);

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private long tag = 0;

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
            cache[msgNum / 2] = halfByte.getByte();
            halfByte.setByte((byte) 0);
        }
        msgNum++;
    }

    public void init() {
        //cache.put(msgNum / 2, halfByte.getByte());
        cache[msgNum / 2] = halfByte.getByte();
        //System.out.println("time max:" + max + " count256:" + count256 + " count65536:" + count65536 + " count15:" + count15);
        System.out.println("msgnum:" + msgNum);
        init = true;
    }

    public int getOffset(long time) {
        int tagIndex = timeTags.tagIndex(time);
        long pTag = timeTags.getTag(tagIndex);
        int pOffset = timeTags.getOffset(tagIndex);
        long pTime = pTag;
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
}
