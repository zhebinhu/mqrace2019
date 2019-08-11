package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/09.
 */
public class ValueTags {
    private int[] tags;

    private int[] offsets;

    private int[] adds;

    private int index;

    public ValueTags(int cap) {
        tags = new int[cap];
        offsets = new int[cap];
        adds = new int[cap];
        index = 0;
    }

    public void add(int add) {
        adds[index - 1] = add;
    }

    public int getAdd(int addIndex) {
        return adds[addIndex];
    }

    public void add(int tag, int offset) {
        tags[index] = tag;
        offsets[index] = offset;
        index++;
    }

    public int tagIndex(int tag) {
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

    public int getTag(int tagIndex) {
        return tags[tagIndex];
    }

    public int getOffset(int offsetIndex) {
        return offsets[offsetIndex];
    }

    public int size() {
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
