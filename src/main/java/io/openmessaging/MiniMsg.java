package io.openmessaging;

/**
 * Created by huzhebin on 2019/07/30.
 */
public class MiniMsg {
    //随机整数,可能重复
    private long a;

    //输入时间戳,基本是升序的,可能重复
    private long t;

    public long getA() {
        return a;
    }

    public void setA(long a) {
        this.a = a;
    }

    public long getT() {
        return t;
    }

    public void setT(long t) {
        this.t = t;
    }
}
