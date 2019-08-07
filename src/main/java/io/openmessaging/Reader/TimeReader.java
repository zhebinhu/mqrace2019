package io.openmessaging.Reader;

import io.openmessaging.Message;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {
    private long max = 0;

    private long min = Long.MAX_VALUE;

    public void put(Message message) {
        long t = message.getT();
        if (t > max) {
            max = t;
        }
        if (t < min) {
            min = t;
        }
    }

    public void get() {
        System.out.println("time max:"+max+" min:"+min);
    }
}
