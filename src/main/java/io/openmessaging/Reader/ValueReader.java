package io.openmessaging.Reader;

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

    private int offsetA = 0;

    private int offsetB = 0;

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
        tag = 0;
        halfByte.setByte((byte) 0);
        System.out.println("value max:" + max + " valueTags size:" + valueTags.size());
        init = true;
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
            int tagIndex = valueTags.offsetIndex(offset);
            tag = valueTags.getTag(tagIndex);
            offsetA = valueTags.getOffset(tagIndex);
            if (tagIndex == valueTags.size() - 1) {
                offsetB = msgNum;
            } else {
                offsetB = valueTags.getOffset(tagIndex + 1);
            }
        }
        if (offset % 2 == 0) {
            return tag + HalfByte.getRight(byteBuffer.get(offset / 2));
        } else {
            return tag + HalfByte.getLeft(byteBuffer.get(offset / 2));
        }
    }
}
