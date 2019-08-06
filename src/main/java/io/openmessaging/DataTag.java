package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/02.
 */
public class DataTag implements Comparable<DataTag> {
    byte[] data;

    int offset;

    DataTag(byte[] data, int offset) {
        this.data = data;
        this.offset = offset;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public int compareTo(DataTag o) {
        if (offset > o.getOffset()) {
            return 1;
        } else if (offset < o.getOffset()) {
            return -1;
        } else {
            return 0;
        }
    }
}