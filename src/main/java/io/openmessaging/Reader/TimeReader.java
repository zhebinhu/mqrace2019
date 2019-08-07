package io.openmessaging.Reader;

import io.openmessaging.Message;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class TimeReader {
    private long max = 0;

    private long min = Long.MAX_VALUE;

    private long tag = 0;

    private long tagCount = 0;

    private long tag2 = 0;

    private long tag2Count = 0;

    public void put(Message message) {
        long t = message.getT();
        if (t > max) {
            max = t;
        }
        if (t < min) {
            min = t;
        }
        if (t - tag > 15) {
            tag = t;
            tagCount++;
        }
        if (t != tag2) {
            tag2 = t;
            tag2Count++;
        }
    }

    public void get() {
        System.out.println("time max:" + max + " min:" + min + " tagCount:" + tagCount + " tag2Count:" + tag2Count);
    }
}
