package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class TimeTags {
    private long tagsBase;

    private long offsetsBase;

    private int index;

    public TimeTags(int cap) {
        tagsBase = UnsafeWrapper.unsafe.allocateMemory(cap * 8);
        offsetsBase = UnsafeWrapper.unsafe.allocateMemory(cap * 4);
        index = 0;
    }

    public void add(long tag, int offset) {
        UnsafeWrapper.unsafe.putLong(tagsBase + index * 8, tag);
        UnsafeWrapper.unsafe.putInt(offsetsBase + index * 4, offset);
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
        return UnsafeWrapper.unsafe.getLong(tagsBase + tagIndex * 8);
    }

    public int getOffset(int offsetIndex) {
        return UnsafeWrapper.unsafe.getInt(offsetsBase + offsetIndex * 4);
    }

    public int size() {
        return index;
    }

    private int binarySearchTag(long key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = UnsafeWrapper.unsafe.getLong(tagsBase + mid * 8);

            if (midVal + 15 < key) {
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
