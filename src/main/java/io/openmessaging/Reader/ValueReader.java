package io.openmessaging.Reader;

import io.openmessaging.Context;
import io.openmessaging.Message;
import io.openmessaging.ValueTags;

import java.util.*;
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

    private long pre32Add = 0L;

    private Deque<Integer> pre32Queue = new LinkedList<>();


    public void put(Message message) {
        int value = (int) message.getA();
        if (tag == -1 || value > tag + 255 || value < tag) {
            if (valueTags.size() > 0) {
                int endIndex = valueTags.size() - 1;
                pre32Add += (msgNum - valueTags.getOffset(endIndex)) * (long) valueTags.getTag(endIndex) + add;
            }
            if (valueTags.size() > 511) {
                int start32Index = valueTags.size() - 512;
                valueTags.add32(pre32Add, Collections.max(pre32Queue), Collections.min(pre32Queue), start32Index);
                pre32Add -= ((valueTags.getOffset(start32Index + 1) - valueTags.getOffset(start32Index)) * (long) valueTags.getTag(start32Index) + valueTags.getAdd(start32Index));
                pre32Queue.removeFirst();
            }
            if (add > max) {
                max = add;
            }
            if (add != 0) {
                valueTags.add(add);
                add = 0;
            }
            tag = value;
            pre32Queue.addLast(tag);
            valueTags.add(tag, msgNum);
        }
        add = add + value - tag;
        cache[msgNum] = (byte) (value - tag);
        msgNum++;
    }

    public void init() {
        int endIndex = valueTags.size() - 1;
        pre32Add += (msgNum - valueTags.getOffset(endIndex)) * (long) valueTags.getTag(endIndex) + add;
        if (add != 0) {
            valueTags.add(add);
        }
        for (int i = 0; i < 512; i++) {
            int start32Index = valueTags.size() - 512 + i;
            valueTags.add32(pre32Add, Collections.max(pre32Queue), Collections.min(pre32Queue), start32Index);
            pre32Add -= ((valueTags.getOffset(start32Index + 1) - valueTags.getOffset(start32Index)) * (long) valueTags.getTag(start32Index) + valueTags.getAdd(start32Index));
            pre32Queue.removeFirst();
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
            context.offsetA = valueTags.getOffset(tagIndex);
            if (tagIndex == valueTags.size() - 1) {
                context.offsetB = msgNum;
            } else {
                context.offsetB = valueTags.getOffset(tagIndex + 1);
            }
        }
        return context.tag + (cache[offset] + 256) % 256;
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, Context context) {
        long total = 0;
        int count = 0;
        if (offsetA >= context.offsetB || offsetA < context.offsetA) {
            context.tagIndex = valueTags.offsetIndex(offsetA);
            context.tag = valueTags.getTag(context.tagIndex);
            context.offsetA = valueTags.getOffset(context.tagIndex);
            if (context.tagIndex == valueTags.size() - 1) {
                context.offsetB = msgNum;
            } else {
                context.offsetB = valueTags.getOffset(context.tagIndex + 1);
            }
        }
        int value;
        // 对齐
        while (offsetA != context.offsetA) {
            value = context.tag + (cache[offsetA] + 256) % 256;
            if (value >= aMin && value <= aMax) {
                total += value;
                count++;
            }
            offsetA++;
            if (offsetA == offsetB) {
                return count == 0 ? 0 : total / count;
            }
            if (offsetA >= context.offsetB) {
                context.tagIndex++;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = offsetA;
                if (context.tagIndex == valueTags.size() - 1) {
                    context.offsetB = msgNum;
                } else {
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                }
            }
        }

        if (context.tagIndex < valueTags.size() - 512) {
            context.offsetB32 = valueTags.getOffset(context.tagIndex + 512);
            context.pre32Max = valueTags.getPre32Max(context.tagIndex);
            context.pre32Min = valueTags.getPre32Min(context.tagIndex);
            while (context.pre32Min >= aMin && context.offsetB32 < offsetB && context.pre32Max + 255 <= aMax) {
                int num = context.offsetB32 - context.offsetA;
                total += valueTags.getPre32(context.tagIndex);
                count += num;
                offsetA = context.offsetB32;
                if (offsetA == offsetB) {
                    return count == 0 ? 0 : total / count;
                }
                context.tagIndex += 512;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = context.offsetB32;
                if (context.tagIndex >= valueTags.size() - 512) {
                    break;
                } else {
                    context.offsetB32 = valueTags.getOffset(context.tagIndex + 512);
                    if (context.offsetB32 < offsetB) {
                        context.pre32Max = valueTags.getPre32Max(context.tagIndex);
                        context.pre32Min = valueTags.getPre32Min(context.tagIndex);
                    } else {
                        break;
                    }
                }
            }
        }

        context.offsetA = valueTags.getOffset(context.tagIndex);
        if (context.tagIndex == valueTags.size() - 1) {
            context.offsetB = msgNum;
        } else {
            context.offsetB = valueTags.getOffset(context.tagIndex + 1);
        }

        while (offsetA < offsetB) {
            if (offsetA >= context.offsetB) {
                context.tagIndex++;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = valueTags.getOffset(context.tagIndex);
                if (context.tagIndex == valueTags.size() - 1) {
                    context.offsetB = msgNum;
                } else {
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                }
            }

            if (context.offsetA == offsetA && context.offsetB < offsetB && context.tag + 255 <= aMax && context.tag >= aMin) {
                int num = context.offsetB - context.offsetA;
                total += num * (long) context.tag + valueTags.getAdd(context.tagIndex);
                count += num;
                offsetA = context.offsetB;
                context.tagIndex++;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = valueTags.getOffset(context.tagIndex);
                if (context.tagIndex == valueTags.size() - 1) {
                    context.offsetB = msgNum;
                } else {
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                }
                continue;
            }
            value = context.tag + (cache[offsetA] + 256) % 256;
            if (value >= aMin && value <= aMax) {
                total += value;
                count++;
            }
            offsetA++;
        }
        return count == 0 ? 0 : total / count;
    }
}
