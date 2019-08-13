package io.openmessaging.Reader;

import io.openmessaging.Context;
import io.openmessaging.Message;
import io.openmessaging.ValueTags;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class ValueReader {
    private long max = 0;

    private byte[] cache = new byte[Integer.MAX_VALUE - 2];

    private ValueTags valueTags = new ValueTags(10000000);

    private int msgNum = 0;

    private volatile boolean init = false;

    private int tag = -1;

    private int add = 0;

    private long total = 0;

    private int count = 0;

    private AtomicInteger c = new AtomicInteger();

    private AtomicInteger c1 = new AtomicInteger();

    private AtomicInteger c2 = new AtomicInteger();

    public void put(Message message) {
        int value = (int) message.getA();
        if (tag == -1 || value > tag + 255 || value < tag) {
            if (add != 0) {
                valueTags.add(add);
                add = 0;
            }
            if (value == tag + 256 && count < 32) {
                count++;
            } else {
                valueTags.addTotal(total, count);
                count = 1;
                total = 0;
            }
            tag = value;
            valueTags.add(value, msgNum);
        }
        add = add + value - tag;
        total = total + value;
        cache[msgNum] = (byte) (value - tag);
        msgNum++;
    }

    public void init() {
        if (add != 0) {
            valueTags.add(add);
            valueTags.addTotal(total, count);
            valueTags.addFinal(0, msgNum);
        }
        System.out.println("max:" + max + " valueTags size:" + valueTags.size());
        init = true;
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
            int tagIndex = valueTags.offsetIndex(offset);
            context.tag = valueTags.getTag(tagIndex);
            context.offsetA = valueTags.getOffset(context.tagIndex);
            context.offsetB = valueTags.getOffset(context.tagIndex + 1);
            context.offsetC = valueTags.getOffset(context.tagIndex + valueTags.getJump(context.tagIndex));
        }
        return context.tag + (cache[offset] & 0xff);
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, Context context) {
        long total = 0;
        int count = 0;
        if (offsetA < context.offsetA || offsetA >= context.offsetB) {
            context.tagIndex = valueTags.offsetIndex(offsetA);
            context.tag = valueTags.getTag(context.tagIndex);
            context.offsetA = valueTags.getOffset(context.tagIndex);
            context.offsetB = valueTags.getOffset(context.tagIndex + 1);
            context.offsetC = valueTags.getOffset(context.tagIndex + valueTags.getJump(context.tagIndex));
        }
        while (offsetA < offsetB) {
            c.getAndIncrement();
            if (offsetA >= context.offsetB) {
                context.tagIndex++;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = valueTags.getOffset(context.tagIndex);
                context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                context.offsetC = valueTags.getOffset(context.tagIndex + valueTags.getJump(context.tagIndex));
            }
            if (context.offsetA == offsetA && context.tag >= aMin) {
                if (context.tag + 256 * valueTags.getJump(context.tagIndex) <= aMax && context.offsetC < offsetB) {
                    c1.getAndIncrement();
                    int num = context.offsetC - context.offsetA;
                    total += valueTags.getTotal(context.tagIndex);
                    count += num;
                    offsetA = context.offsetC;
                    context.tagIndex += valueTags.getJump(context.tagIndex);
                    context.tag = valueTags.getTag(context.tagIndex);
                    context.offsetA = valueTags.getOffset(context.tagIndex);
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                    context.offsetC = valueTags.getOffset(context.tagIndex + valueTags.getJump(context.tagIndex));
                    continue;
                }
                if (context.tag + 255 <= aMax && context.offsetB < offsetB) {
                    c2.getAndIncrement();
                    int num = context.offsetB - context.offsetA;
                    total += context.tag * (long) num + valueTags.getAdd(context.tagIndex);
                    count += num;
                    offsetA = context.offsetB;
                    context.tagIndex++;
                    context.tag = valueTags.getTag(context.tagIndex);
                    context.offsetA = valueTags.getOffset(context.tagIndex);
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                    context.offsetC = valueTags.getOffset(context.tagIndex + valueTags.getJump(context.tagIndex));
                    continue;
                }
            }
            int value = context.tag + (cache[offsetA] & 0xff);
            if (value >= aMin && value <= aMax) {
                total += value;
                count++;
            }
            offsetA++;
        }
        System.out.println("c:" + c.intValue() + " c1:" + c1.intValue() + " c2:" + c2.intValue());
        return count == 0 ? 0 : total / count;
    }
}
