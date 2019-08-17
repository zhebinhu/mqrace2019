package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class TimeTags {
    private long[] tags;

    private int[] offsets;

    private int index;

    public TimeTags(int cap) {
        tags = new long[cap];
        offsets = new int[cap];
        index = 0;
    }

    public void add(long tag, int offset) {
        tags[index] = tag;
        offsets[index] = offset;
        index++;
    }

    public int tagIndex(long tag) {
        int tagIndex = binarySearch(tags, tag);
        if (tagIndex < 0) {
            tagIndex = Math.max(0, -(tagIndex + 2));
        }
        return tagIndex;
    }

    public int offsetIndex(int offset) {
        int offsetIndex = binarySearch(offsets, offset);
        if (offsetIndex < 0) {
            offsetIndex = Math.max(0, -(offsetIndex + 2));
        }
        return offsetIndex;
    }

    public long getTag(int tagIndex) {
        return tags[tagIndex];
    }

    public int getOffset(int offsetIndex) {
        return offsets[offsetIndex];
    }

    public int size() {
        return index;
    }

    private int binarySearch(long[] a, long key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = a[mid];

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

    private int binarySearch(int[] a, int key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = a[mid];

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
