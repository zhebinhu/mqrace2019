package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeTag {
    private int offset;

    private int time;

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public TimeTag(int offset, int time) {
        this.offset = offset;
        this.time = time;
    }
}
