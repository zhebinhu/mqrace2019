package io.openmessaging.Reader;

import io.openmessaging.Message;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class ValueReader {
    private long max = 0;

    private long min = Long.MAX_VALUE;

    private long tag = 0;

    private long tagCount = 0;

    private long tag2 = 0;

    private long tag2Count = 0;

    public void put(Message message) {
        long value = message.getA() - message.getT();
        if (value > max) {
            max = value;
        }
        if (value < min) {
            min = value;
        }
        if (value > tag + 15 || value < tag) {
            tag = value;
            tagCount++;
        }
        if (value != tag2) {
            tag2 = value;
            tag2Count++;
        }


    }

    public void get() {
        System.out.println("value max:" + max + " min:" + min + " tagCount:" + tagCount + " tag2Count:" + tag2Count);
    }
}
