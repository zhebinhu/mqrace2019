package io.openmessaging.Reader;

import io.openmessaging.Context;
import io.openmessaging.Message;
import io.openmessaging.ValueTags;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class ValueReader {
    private byte[] cache = new byte[Integer.MAX_VALUE - 2];

    private ValueTags valueTags = new ValueTags(15000000);

    private int msgNum = 0;

    private volatile boolean init = false;

    private int tag = -1;

    private long add = 0;

    public void put(Message message) {
        int value = (int) message.getA();
        if (tag == -1 || value > tag + 127 || value < tag) {
            if (add != 0) {
                valueTags.add(add);
                add = 0;
            }

            tag = value;
            valueTags.add(value, msgNum);
        }
        add = add + value;
        cache[msgNum] = (byte) (value - tag);
        msgNum++;
    }

    public void init() {
        if (add != 0) {
            valueTags.add(add);
            add = 0;
            valueTags.inited(msgNum);
        }
        System.out.println(" valueTags size:" + valueTags.size());
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
            context.offsetA = valueTags.getOffset(tagIndex);
            context.offsetB = valueTags.getOffset(tagIndex + 1);
        }
        return context.tag + cache[offset];
    }

    long avg(int offsetA, int offsetB, long aMin, long aMax, Context context) {
        long total = 0;
        int count = 0;

        if (offsetA < context.offsetA || offsetA >= context.offsetB) {
            context.tagIndex = valueTags.offsetIndex(offsetA);
            context.tag = valueTags.getTag(context.tagIndex);
        }
        while (context.tag + 127 < aMin && offsetA < offsetB) {
            context.tagIndex++;
            context.tag = valueTags.getTag(context.tagIndex);
            offsetA = valueTags.getOffset(context.tagIndex);
        }
        context.offsetA = valueTags.getOffset(context.tagIndex);
        context.offsetB = valueTags.getOffset(context.tagIndex + 1);

        while (offsetA < offsetB) {
            if (offsetA >= context.offsetB && upDateContext(aMax, context)) {
                break;
            }
            if (context.offsetA == offsetA && context.tag + 127 <= aMax && context.tag >= aMin && context.offsetB < offsetB) {
                int num = context.offsetB - context.offsetA;
                total += valueTags.getAdd(context.tagIndex);
                count += num;
                offsetA = context.offsetB;
                if (upDateContext(aMax, context)) {
                    break;
                }
                continue;
            }
            int value = context.tag + cache[offsetA];
            if (value >= aMin && value <= aMax) {
                total += value;
                count++;
            }
            offsetA++;
        }
        return count == 0 ? 0 : total / count;
    }

    private boolean upDateContext(long aMax, Context context) {
        context.tagIndex++;
        if (valueTags.getMin(context.tagIndex) > aMax) {
            context.tagIndex--;
            return true;
        }
        context.tag = valueTags.getTag(context.tagIndex);
        context.offsetA = valueTags.getOffset(context.tagIndex);
        context.offsetB = valueTags.getOffset(context.tagIndex + 1);
        return false;
    }
}
