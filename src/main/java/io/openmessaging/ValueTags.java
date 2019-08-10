package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/09.
 */
public class ValueTags{
    private ByteBuffer tags;

    private ByteBuffer offsets;

    private ByteBuffer adds;

    private int index;

    public ValueTags(int cap) {
        tags = ByteBuffer.allocateDirect(4 * cap);
        offsets = ByteBuffer.allocateDirect(4 * cap);
        index = 0;
        adds = ByteBuffer.allocateDirect(cap);
    }

    public void add(byte add) {
        adds.put(index - 1, add);
    }

    public byte getAdd(int addIndex) {
        return adds.get(addIndex);
    }



    public void add(int tag, int offset) {
        tags.putInt(index << 2, tag);
        offsets.putInt(index << 2, offset);
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
        return tags.getInt(tagIndex << 2);
    }

    public int getOffset(int offsetIndex) {
        return offsets.getInt(offsetIndex << 2);
    }

    public int size() {
        return index;
    }

    private int binarySearch(ByteBuffer a, int key) {
        int low = 0;
        int high = index - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = a.getInt(mid << 2);

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
