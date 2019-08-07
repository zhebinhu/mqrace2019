package io.openmessaging.Reader;

import io.openmessaging.Message;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class ValueReader {
    private long max = 0;

    private long min = Long.MAX_VALUE;

    public void put(Message message) {
        long a = message.getA();
        if (a > max) {
            max = a;
        }
        if (a < min) {
            min = a;
        }
    }

    public void get() {
        System.out.println("value max:"+max+" min:"+min);
    }
}
