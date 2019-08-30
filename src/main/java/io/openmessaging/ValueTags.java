package io.openmessaging;

import io.openmessaging.Context.ValueContext;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class ValueTags {
    private ByteBuffer reals;

    private ByteBuffer offsets;

    private ByteBuffer tags;

    private int index;

    public ValueTags(int cap) {
        reals = ByteBuffer.allocateDirect(cap * 8);
        offsets = ByteBuffer.allocateDirect(cap * 4);
        tags = ByteBuffer.allocateDirect(cap);
        index = 0;
    }

    public void add(long real, int offset, byte tag) {
        reals.putLong(real);
        offsets.putInt(offset);
        tags.put(tag);
        index++;
    }

    public long getRealOffset(int offset, ValueContext valueContext) {
        int realIndex = binarySearchOffset(offset);
        if (realIndex < 0) {
            realIndex = Math.max(0, -(realIndex + 2));
        }
        valueContext.tagIndex = realIndex;
        long real = reals.getLong(realIndex * 8);
        int tagOffset = offsets.getInt(realIndex * 4);
        if (realIndex == index - 1) {
            valueContext.nextOffset = Integer.MAX_VALUE;
        } else {
            valueContext.nextOffset = offsets.getInt( (realIndex + 1) * 4);
        }
        byte tag = tags.get( realIndex);
        valueContext.tag = tag;

        return real + (long) tag * (offset - tagOffset);
    }

    public long getRealOffset(int offset) {
        int realIndex = binarySearchOffset(offset);
        if (realIndex < 0) {
            realIndex = Math.max(0, -(realIndex + 2));
        }
        long real = reals.getLong(realIndex * 8);
        int tagOffset = offsets.getInt(realIndex * 4);
        byte tag = tags.get( realIndex);
        return real + (long) tag * (offset - tagOffset);
    }

    public void update(ValueContext valueContext) {
        valueContext.tagIndex = valueContext.tagIndex + 1;
        valueContext.tag = tags.get(valueContext.tagIndex);
        if (valueContext.tagIndex == index - 1) {
            valueContext.nextOffset = Integer.MAX_VALUE;
        } else {
            valueContext.nextOffset = offsets.getInt((valueContext.tagIndex + 1) * 4);
        }
    }

    public int size() {
        return index - 1;
    }

    private int binarySearchOffset(int key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = offsets.getInt(mid * 4);

            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

}
