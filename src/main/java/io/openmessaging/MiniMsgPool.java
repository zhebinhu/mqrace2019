package io.openmessaging;

/**
 * Created by huzhebin on 2019/07/30.
 */
public class MiniMsgPool {
    MiniMsg[] miniMsgs = new MiniMsg[1000000];

    volatile int index = 0;

    MiniMsgPool() {
        for (int i = 0; i < 1000000; i++) {
            miniMsgs[i] = new MiniMsg();
        }
    }

    public synchronized MiniMsg get() {
        index = (index + 1) % 1000000;
        return miniMsgs[index];
    }
}
