package io.openmessaging;

/**
 * Created by huzhebin on 2019/07/31.
 */
public class TimeTag2 implements Comparable<TimeTag2> {
    long time;

    int offset;

    TimeTag2(long time, int offset) {
        this.time = time;
        this.offset = offset;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public int compareTo(TimeTag2 o) {
        if (time > o.getTime()) {
            return 1;
        } else if (time < o.getTime()) {
            return -1;
        } else {
            return 0;
        }
    }
}
