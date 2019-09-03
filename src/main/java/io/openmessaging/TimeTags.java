package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class TimeTags {
    private ByteBuffer tagsBase;

    private int[] offsets;

    private int index;

    public TimeTags(int cap) {
        tagsBase = ByteBuffer.allocateDirect(cap * 8);
        offsets = new int[cap];
        index = 0;
    }

    public void add(long tag, int offset) {
        tagsBase.putLong(index * 8,tag);
        offsets[index] = offset;
        index++;
    }

    public int tagIndex(long tag) {
        int tagIndex = binarySearchTag(tag);
        if (tagIndex < 0) {
            tagIndex = Math.max(0, -(tagIndex + 2));
        }
        return tagIndex;
    }

    public int offsetIndex(int offset) {
        int offsetIndex = binarySearchOffset(offset);
        if (offsetIndex < 0) {
            offsetIndex = Math.max(0, -(offsetIndex + 2));
        }
        return offsetIndex;
    }

    public long getTag(int tagIndex) {
        return tagsBase.getLong(tagIndex * 8);
    }

    public int getOffset(int offsetIndex) {
        return offsets[offsetIndex];
    }

    public int size() {
        return index;
    }

    private int binarySearchTag(long key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = tagsBase.getLong(mid * 8);

            if (midVal + 255 < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private int binarySearchOffset(int key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = offsets[mid];
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
