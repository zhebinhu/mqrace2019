package io.openmessaging.Reader;

import io.openmessaging.Context;
import io.openmessaging.Message;
import io.openmessaging.ValueTags;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class ValueReader {
    private long max = 0;

    private byte[] cache = new byte[2100000000];

    private ValueTags valueTags = new ValueTags(10000000);

    private int msgNum = 0;

    private volatile boolean init = false;

    private int tag = -1;

    private int add = 0;

    void put(Message message) {
        int value = (int) message.getA();
        if (tag == -1 || value > tag + 255 || value < tag) {
            if (add > max) {
                max = add;
            }
            if (add != 0) {
                valueTags.addFinal(add, msgNum);
                add = 0;
            }
            tag = value;
            valueTags.add(value, msgNum);
        }
        add = add + value - tag;
        cache[msgNum] = (byte) (value - tag);
        msgNum++;
    }

    private void init() {
        if (add != 0) {
            valueTags.addFinal(add, msgNum);
            add = 0;
        }
        System.out.println("max:" + max + " valueTags size:" + valueTags.size());
        init = true;
    }

    int get(int offset, Context context) {
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
            context.offsetA = valueTags.getOffset(tagIndex);
            if (tagIndex == valueTags.size() - 1) {
                context.offsetB = msgNum;
            } else {
                context.offsetB = valueTags.getOffset(tagIndex + 1);
            }
        }
        return context.tag + (cache[offset] & 0xff);
    }

    long avg(int offsetA, int offsetB, long aMin, long aMax, Context context) {
        long total = 0;
        int count = 0;
        if (offsetA < context.offsetA || offsetA >= context.offsetB) {
            context.tagIndex = valueTags.offsetIndex(offsetA);
            updateParam(context);
        }
        int value;
        while (offsetA < offsetB) {
            if (offsetA >= context.offsetB) {
                updateContext(context);
            }
            if (context.offsetA == offsetA && context.tag + 255 <= aMax && context.tag >= aMin && context.offsetB < offsetB) {
                int num = context.offsetB - context.offsetA;
                total += valueTags.getAdd(context.tagIndex);
                count += num;
                offsetA = context.offsetB;
                updateContext(context);
                continue;
            }
            value = context.tag + (cache[offsetA] & 0xff);
            if (value >= aMin && value <= aMax) {
                total += value;
                count++;
            }
            offsetA++;
        }
        return count == 0 ? 0 : total / count;
    }

    private void updateContext(Context context) {
        context.tagIndex++;
        updateParam(context);
    }

    private void updateParam(Context context) {
        context.tag = valueTags.getTag(context.tagIndex);
        context.offsetA = valueTags.getOffset(context.tagIndex);
        if (context.tagIndex == valueTags.size() - 1) {
            context.offsetB = msgNum;
        } else {
            context.offsetB = valueTags.getOffset(context.tagIndex + 1);
        }
    }
}
