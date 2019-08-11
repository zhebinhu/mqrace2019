package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/09.
 */
public class ValueTags {
    private int[] tags;

    private int[] offsets;

    private long[] adds;

    private int index;

    public ValueTags(int cap) {
        tags = new int[cap];
        offsets = new int[cap];
        adds = new long[cap];
        index = 0;
    }

    public void addFinal(int add, int msgNum) {
        adds[index - 1] = tags[index - 1] * (long) (msgNum - offsets[index - 1]) + add;
    }

    public final long getAdd(int addIndex) {
        return adds[addIndex];
    }

    public void add(int tag, int offset) {
        tags[index] = tag;
        offsets[index] = offset;
        index++;
    }

    public int offsetIndex(int offset) {
        int offsetIndex = binarySearch(offsets, offset);
        if (offsetIndex < 0) {
            offsetIndex = Math.max(0, -(offsetIndex + 2));
        }
        return offsetIndex;
    }

    public final int getTag(int tagIndex) {
        return tags[tagIndex];
    }

    public final int getOffset(int offsetIndex) {
        return offsets[offsetIndex];
    }

    public final int size() {
        return index;
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
