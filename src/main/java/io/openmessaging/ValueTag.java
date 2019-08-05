package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/05.
 */
public class ValueTag implements Comparable<ValueTag>{
    int value;
    int offset;

    public ValueTag(int value, int offset) {
        this.value = value;
        this.offset = offset;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public int compareTo(ValueTag o) {
        if (offset > o.getOffset()) {
            return 1;
        } else if (offset < o.getOffset()) {
            return -1;
        } else {
            return 0;
        }
    }
}
