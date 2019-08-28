package io.openmessaging;

import io.openmessaging.Context.ValueContext;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class ValueTags {
    private long realBase;

    private long offsetsBase;

    private long tagsBase;

    private int index;

    public ValueTags(int cap) {
        realBase = UnsafeWrapper.unsafe.allocateMemory(cap * 8);
        offsetsBase = UnsafeWrapper.unsafe.allocateMemory(cap * 4);
        tagsBase = UnsafeWrapper.unsafe.allocateMemory(cap);
        index = 0;
    }

    public void add(long real, int offset, byte tag) {
        UnsafeWrapper.unsafe.putLong(realBase + index * 8, real);
        UnsafeWrapper.unsafe.putInt(offsetsBase + index * 4, offset);
        UnsafeWrapper.unsafe.putByte(tagsBase + index, tag);
        index++;
    }

    public long getRealOffset(int offset, ValueContext valueContext) {
        int realIndex = binarySearchOffset(offset);
        if (realIndex < 0) {
            realIndex = Math.max(0, -(realIndex + 2));
        }
        valueContext.tagIndex = realIndex;
        long real = UnsafeWrapper.unsafe.getLong(realBase + realIndex * 8);
        int tagOffset = UnsafeWrapper.unsafe.getInt(offsetsBase + realIndex * 4);
        if (realIndex == index - 1) {
            valueContext.nextOffset = Integer.MAX_VALUE;
        } else {
            valueContext.nextOffset = UnsafeWrapper.unsafe.getInt(offsetsBase + (realIndex + 1) * 4);
        }
        byte tag = UnsafeWrapper.unsafe.getByte(tagsBase + realIndex);
        valueContext.tag = tag;

        return real + (long) tag * 2 * (offset - tagOffset);
    }

    public long getRealOffset(int offset) {
        int realIndex = binarySearchOffset(offset);
        if (realIndex < 0) {
            realIndex = Math.max(0, -(realIndex + 2));
        }
        long real = UnsafeWrapper.unsafe.getLong(realBase + realIndex * 8);
        int tagOffset = UnsafeWrapper.unsafe.getInt(offsetsBase + realIndex * 4);
        byte tag = UnsafeWrapper.unsafe.getByte(tagsBase + realIndex);
        return real + (long) tag * 2 * (offset - tagOffset);
    }

    public void update(ValueContext valueContext) {
        valueContext.tagIndex = valueContext.tagIndex + 1;
        valueContext.tag = UnsafeWrapper.unsafe.getByte(tagsBase + valueContext.tagIndex);
        if (valueContext.tagIndex == index - 1) {
            valueContext.nextOffset = Integer.MAX_VALUE;
        } else {
            valueContext.nextOffset = UnsafeWrapper.unsafe.getInt(offsetsBase + (valueContext.tagIndex + 1) * 4);
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
            int midVal = UnsafeWrapper.unsafe.getInt(offsetsBase + mid * 4);

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
