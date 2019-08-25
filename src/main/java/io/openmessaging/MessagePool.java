package io.openmessaging;

/**
 * Created by huzhebin on 2019/07/28.
 */
public class MessagePool {
    public static MessagePool[] messagePools = new MessagePool[12];

    static {
        for (int i = 0; i < 12; i++) {
            messagePools[i] = new MessagePool();
        }
    }

    public static int count = 0;

    public synchronized static MessagePool getPool() {
        return messagePools[count++];
    }

    Message[] messages = new Message[160000];

    private int index = 0;

    MessagePool() {
        for (int i = 0; i < 160000; i++) {
            messages[i] = new Message(0, 0, new byte[34]);
        }
    }

    public Message get() {
        index = (index + 1) % 160000;
        return messages[index];
    }
}
