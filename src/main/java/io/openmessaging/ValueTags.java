package io.openmessaging;

import io.openmessaging.Context.ValueContext;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class ValueTags {
    private long[] realBase;

    private int[] offsetsBase;

    private byte[] tagsBase;

    private int index;

    private int cap;

    public ValueTags(int cap) {
        realBase = new long[cap * 8];
        offsetsBase = new int[cap * 4];
        tagsBase = new byte[cap];
        this.cap = cap;
        index = 0;
    }

    public void add(long real, int offset, byte tag) {
        if (index >= cap) {
            cap = cap + 100000;
            realBase = Arrays.copyOf(realBase,cap);
            offsetsBase = Arrays.copyOf(offsetsBase,cap);
            tagsBase = Arrays.copyOf(tagsBase,cap);
        }
        realBase[index] = real;
        offsetsBase[index] = offset;
        tagsBase[index] = tag;
        index++;
    }

    public long getRealOffset(int offset, ValueContext valueContext) {
        int realIndex = binarySearchOffset(offset);
        if (realIndex < 0) {
            realIndex = Math.max(0, -(realIndex + 2));
        }
        valueContext.tagIndex = realIndex;
        long real = realBase[realIndex];
        int tagOffset = offsetsBase[realIndex];
        if (realIndex == index - 1) {
            valueContext.nextOffset = Integer.MAX_VALUE;
        } else {
            valueContext.nextOffset = offsetsBase[(realIndex + 1)];
        }
        byte tag = tagsBase[realIndex];
        valueContext.tag = tag;

        return real + (long) tag*2 * (offset - tagOffset);
    }

    public long getRealOffset(int offset) {
        int realIndex = binarySearchOffset(offset);
        if (realIndex < 0) {
            realIndex = Math.max(0, -(realIndex + 2));
        }
        long real = realBase[realIndex];
        int tagOffset = offsetsBase[realIndex];
        byte tag = tagsBase[realIndex];
        return real + (long) tag*2 * (offset - tagOffset);
    }

    public void update(ValueContext valueContext) {
        valueContext.tagIndex = valueContext.tagIndex + 1;
        valueContext.tag = tagsBase[valueContext.tagIndex];
        if (valueContext.tagIndex == index - 1) {
            valueContext.nextOffset = Integer.MAX_VALUE;
        } else {
            valueContext.nextOffset = offsetsBase[(valueContext.tagIndex + 1)];
        }
    }

    public int size() {
        return index;
    }

    private int binarySearchOffset(int key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = offsetsBase[mid];

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
