package io.openmessaging.Reader;

import io.openmessaging.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {
    private long max = 0;

    private byte[] cache = new byte[Integer.MAX_VALUE / 2];

    private List<TimeTag> timeTags = new ArrayList<>();

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private ThreadLocal<Integer> offsetA = new ThreadLocal<>();

    private ThreadLocal<Integer> offsetB = new ThreadLocal<>();

    private ThreadLocal<Integer> tag = new ThreadLocal<>();

    private ThreadLocal<TimeTag> timeTag = new ThreadLocal<>();

    public void put(Message message) {
        long t = message.getT();
        if (t > max) {
            max = t;
        }
        int time = (int) t;
        if (tag.get() == null || time > tag.get() + 15) {
            tag.set(time);
            timeTags.add(new TimeTag(msgNum, time));
        }
        if (msgNum % 2 == 0) {
            halfByte.setRight((byte) (time - tag.get()));
        } else {
            halfByte.setLeft((byte) (time - tag.get()));
            cache[msgNum / 2] = halfByte.getByte();
            halfByte.setByte((byte) 0);
        }
        msgNum++;
    }

    public void init() {
        cache[msgNum / 2] = halfByte.getByte();
        tag.set(0);
        halfByte.setByte((byte) 0);
        timeTag.set(new TimeTag(0,0));
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
        TimeTag tmpTimeTag = timeTag.get();
        tmpTimeTag.setTime(time);
        int tagIndex = Collections.binarySearch(timeTags, tmpTimeTag, Comparator.comparingInt(TimeTag::getTime));
        if (tagIndex < 0) {
            tagIndex = Math.max(0, -(tagIndex + 2));
        }
        int pTag = timeTags.get(tagIndex).getTime();
        int pOffset = timeTags.get(tagIndex).getOffset();
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

    public int get(int offset) {
        if (!init) {
            synchronized (this) {
                if (!init) {
                    init();
                }
            }
        }
        if (offsetA.get() == null) {
            offsetA.set(0);
        }
        if (offsetB.get() == null) {
            offsetB.set(0);
        }
        TimeTag tmpTimeTag = timeTag.get();
        tmpTimeTag.setOffset(offset);
        if (offset < offsetA.get() || offset >= offsetB.get()) {
            int tagIndex = Collections.binarySearch(timeTags, tmpTimeTag, Comparator.comparingInt(TimeTag::getOffset));
            if (tagIndex < 0) {
                tagIndex = Math.max(0, -(tagIndex + 2));
            }
            tag.set(timeTags.get(tagIndex).getTime());
            offsetA.set(timeTags.get(tagIndex).getOffset());
            if (tagIndex == timeTags.size() - 1) {
                offsetB.set(msgNum);
            } else {
                offsetB.set(timeTags.get(tagIndex + 1).getOffset());
            }
        }
        if (offset % 2 == 0) {
            return tag.get() + HalfByte.getRight(cache[offset / 2]);
        } else {
            return tag.get() + HalfByte.getLeft(cache[offset / 2]);
        }
    }
}
