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

    private int tag = 0;

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private int offsetA = 0;

    private int offsetB = 0;

    public void put(Message message) {
        long t = message.getT();
        if (t > max) {
            max = t;
        }
        int time = (int) t;
        if (time > tag + 15) {
            tag = time;
            timeTags.add(new TimeTag(msgNum, time));
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
        tag = 0;
        halfByte.setByte((byte) 0);
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
        int tagIndex = Collections.binarySearch(timeTags, new TimeTag(0, time), Comparator.comparingInt(TimeTag::getTime));
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
        if (offset < offsetA || offset >= offsetB) {
            int tagIndex = Collections.binarySearch(timeTags, new TimeTag(offset, 0), Comparator.comparingInt(TimeTag::getOffset));
            if (tagIndex < 0) {
                tagIndex = Math.max(0, -(tagIndex + 2));
            }
            tag = timeTags.get(tagIndex).getTime();
            offsetA = timeTags.get(tagIndex).getOffset();
            if (tagIndex == timeTags.size() - 1) {
                offsetB = msgNum;
            } else {
                offsetB = timeTags.get(tagIndex + 1).getOffset();
            }
        }
        if (offset % 2 == 0) {
            return tag + HalfByte.getRight(cache[offset / 2]);
        } else {
            return tag + HalfByte.getLeft(cache[offset / 2]);
        }
    }
}
