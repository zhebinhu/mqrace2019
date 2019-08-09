package io.openmessaging;

/**
 * Created by huzhebin on 2019/07/28.
 */
public class MessagePool {
    Message[] messages = new Message[600000];

    volatile int index = 0;

    MessagePool() {
        for (int i = 0; i < 600000; i++) {
            messages[i] = new Message(0, 0, new byte[34]);
        }
    }

    public Message get() {
        index = (index + 1) % 600000;
        return messages[index];
    }
}
