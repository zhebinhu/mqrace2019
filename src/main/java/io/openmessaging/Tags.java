package io.openmessaging;

import java.util.Arrays;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class Tags {
    private int[] tags;

    private int[] offsets;

    private int index;

    private int cap;

    public Tags(int cap) {
        this.cap = cap;
        tags = new int[cap];
        offsets = new int[cap];
        index = 0;
    }

    public void add(int tag, int offset) {
        if (index >= cap) {
            cap += 2000000;
            tags = Arrays.copyOf(tags, cap);
            offsets = Arrays.copyOf(offsets, cap);
        }
        tags[index] = tag;
        offsets[index] = offset;
        index++;
    }

    public int tagIndex(int tag) {
        int tagIndex = Arrays.binarySearch(tags, 0, index, tag);
        if (tagIndex < 0) {
            tagIndex = Math.max(0, -(tagIndex + 2));
        }
        return tagIndex;
    }

    public int offsetIndex(int offset) {
        int tagIndex = Arrays.binarySearch(offsets, 0, index, offset);
        if (tagIndex < 0) {
            tagIndex = Math.max(0, -(tagIndex + 2));
        }
        return tagIndex;
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
}
