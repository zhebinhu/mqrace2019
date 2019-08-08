package io.openmessaging.Reader;

import io.openmessaging.Context;
import io.openmessaging.HalfByte;
import io.openmessaging.Message;
import io.openmessaging.Tags;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class ValueReader {
    private long max = 0;

    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Integer.MAX_VALUE / 2);

    private Tags valueTags = new Tags(2000000);

    private int msgNum = 0;

    private HalfByte halfByte = new HalfByte((byte) 0);

    private volatile boolean init = false;

    private int tag = -1;

    public void put(Message message) {
        long v = message.getA() - message.getT();
        if (v > max) {
            max = v;
        }
        int value = (int) v;
        if (tag == -1 || value > tag + 15 || value < tag) {
            tag = value;
            valueTags.add(value, msgNum);
        }
        if (msgNum % 2 == 0) {
            halfByte.setRight((byte) (value - tag));
        } else {
            halfByte.setLeft((byte) (value - tag));
            byteBuffer.put(msgNum / 2, halfByte.getByte());
            halfByte.setByte((byte) 0);
        }
        msgNum++;
    }

    public void init() {
        byteBuffer.put(msgNum / 2, halfByte.getByte());
        System.out.println("value max:" + max + " valueTags size:" + valueTags.size());
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
        if (offset % 2 == 0) {
            return context.tag + HalfByte.getRight(byteBuffer.get(offset / 2));
        } else {
            return context.tag + HalfByte.getLeft(byteBuffer.get(offset / 2));
        }
    }
}
