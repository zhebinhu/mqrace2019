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

    //private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private int tag = -1;

    private int add = 0;

    private long pre32Add = 0L;

    private Deque<Integer> pre32Queue = new LinkedList<>();

    private boolean pre32 = true;

    public void put(Message message) {
        int value = (int) message.getA();
        if (tag == -1 || value > tag + 255 || value < tag) {
            if (valueTags.size() > 0) {
                int endIndex = valueTags.size() - 1;
                pre32Add += (msgNum - valueTags.getOffset(endIndex)) * (long) valueTags.getTag(endIndex) + add;
            }
            if (valueTags.size() > 31) {
                int start32Index = valueTags.size() - 32;
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
        //        if (msgNum % 2 == 0) {
        //            halfByte.setRight((byte) (value - tag));
        //        } else {
        //            halfByte.setLeft((byte) (value - tag));
        //            cache[msgNum / 2] = halfByte.getByte();
        //            halfByte.setByte((byte) 0);
        //        }
        msgNum++;
    }

    public void init() {
        //cache[msgNum / 2] = halfByte.getByte();
        int endIndex = valueTags.size() - 1;
        pre32Add += (msgNum - valueTags.getOffset(endIndex)) * (long) valueTags.getTag(endIndex) + add;
        if (add != 0) {
            valueTags.add(add);
        }
        for (int i = 0; i < 32; i++) {
            int start32Index = valueTags.size() - 32 + i;
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
            if (tagIndex >= valueTags.size() - 32) {
                context.offsetB32 = msgNum;
            } else {
                context.offsetB32 = valueTags.getOffset(tagIndex + 32);
            }
        }
        return context.tag + (cache[offset] + 256) % 256;
        //        if (offset % 2 == 0) {
        //            return context.tag + HalfByte.getRight(cache[offset / 2]);
        //        } else {
        //            return context.tag + HalfByte.getLeft(cache[offset / 2]);
        //        }
    }

    public long avg(int offsetA, int offsetB, long aMin, long aMax, Context context) {
        long total = 0;
        int count = 0;
        context.tagIndex = valueTags.offsetIndex(offsetA);
        context.tag = valueTags.getTag(context.tagIndex);
        context.offsetA = valueTags.getOffset(context.tagIndex);
        if (context.tagIndex == valueTags.size() - 1) {
            context.offsetB = msgNum;
        } else {
            context.offsetB = valueTags.getOffset(context.tagIndex + 1);
        }
        //            if (context.tagIndex >= valueTags.size() - 32) {
        //                context.offsetB32 = msgNum;
        //            } else {
        //                context.offsetB32 = valueTags.getOffset(context.tagIndex + 32);
        //            }
        //            context.pre32Max = valueTags.getPre32Max(context.tagIndex);
        //            context.pre32Min = valueTags.getPre32Min(context.tagIndex);

        int value;
        while (offsetA < offsetB) {
            if (offsetA >= context.offsetB) {
                context.tagIndex++;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = context.offsetB;
                if (context.tagIndex == valueTags.size() - 1) {
                    context.offsetB = msgNum;
                } else {
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                }
                //                if (offsetA == context.offsetA) {
                //                    if (context.tagIndex >= valueTags.size() - 32) {
                //                        context.offsetB32 = msgNum;
                //                    } else {
                //                        context.offsetB32 = valueTags.getOffset(context.tagIndex + 32);
                //                    }
                //                    if (context.offsetB32 < offsetB) {
                //                        context.pre32Max = valueTags.getPre32Max(context.tagIndex);
                //                        context.pre32Min = valueTags.getPre32Min(context.tagIndex);
                //                    }
                //                }
            }

            if (context.offsetA == offsetA && context.offsetB < offsetB && context.tag + 255 <= aMax && context.tag >= aMin) {
                if (context.pre32Min >= aMin && pre32 && context.offsetB32 < offsetB && context.pre32Max + 255 <= aMax) {
                    int num = context.offsetB32 - context.offsetA;
                    total += valueTags.getPre32(context.tagIndex);
                    count += num;
                    offsetA = context.offsetB32;
                    context.tagIndex += 32;
                    context.tag = valueTags.getTag(context.tagIndex);
                    context.offsetA = context.offsetB32;
                    if (context.tagIndex == valueTags.size() - 1) {
                        context.offsetB = msgNum;
                    } else {
                        context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                    }
                    if (context.tagIndex >= valueTags.size() - 32) {
                        pre32 = false;
                        context.offsetB32 = msgNum;
                    } else {
                        context.offsetB32 = valueTags.getOffset(context.tagIndex + 32);
                        if (context.offsetB32 < offsetB) {
                            context.pre32Max = valueTags.getPre32Max(context.tagIndex);
                            context.pre32Min = valueTags.getPre32Min(context.tagIndex);
                        } else {
                            pre32 = false;
                        }
                    }
                    continue;
                }
                int num = context.offsetB - context.offsetA;
                total += num * (long) context.tag + valueTags.getAdd(context.tagIndex);
                count += num;
                offsetA = context.offsetB;
                context.tagIndex++;
                context.tag = valueTags.getTag(context.tagIndex);
                context.offsetA = context.offsetB;
                if (context.tagIndex == valueTags.size() - 1) {
                    context.offsetB = msgNum;
                } else {
                    context.offsetB = valueTags.getOffset(context.tagIndex + 1);
                }
                //                if (offsetA == context.offsetA) {
                if (context.tagIndex >= valueTags.size() - 32) {
                    pre32 = false;
                    context.offsetB32 = msgNum;
                } else {
                    context.offsetB32 = valueTags.getOffset(context.tagIndex + 32);
                    if (context.offsetB32 < offsetB) {
                        context.pre32Max = valueTags.getPre32Max(context.tagIndex);
                        context.pre32Min = valueTags.getPre32Min(context.tagIndex);
                    } else {
                        pre32 = false;
                    }
                }

                //                }
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
