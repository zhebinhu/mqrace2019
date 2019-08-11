package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/09.
 */
public class ValueTags {
    private int[] tags;

    private int[] offsets;

    private int[] adds;

    private int index;

    private long[] pre32;

    private int[] pre32Max;

    private int[] pre32Min;

    public ValueTags(int cap) {
        tags = new int[cap];
        offsets = new int[cap];
        adds = new int[cap];
        pre32 = new long[cap];
        pre32Max = new int[cap];
        pre32Min = new int[cap];
        index = 0;
    }

    public void add32(long add32, int max32, int min32, int pre32Index) {
        pre32[pre32Index] = add32;
        pre32Max[pre32Index] = max32;
        pre32Min[pre32Index] = min32;
    }

    public long getPre32(int pre32Index){
        return pre32[pre32Index];
    }

    public int getPre32Max(int pre32Index){
        return pre32Max[pre32Index];
    }

    public int getPre32Min(int pre32Index){
        return pre32Min[pre32Index];
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
