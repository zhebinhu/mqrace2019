package io.openmessaging;

import java.util.AbstractList;

/**
 * Created by huzhebin on 2019/08/26.
 */
public class MessageList extends AbstractList<Message> {
    private Message[] data = new Message[80000];

    private int size = 0;

    public MessageList() {
        super();
        for (int i = 0; i < 80000; i++) {
            data[i] = new Message(0, 0, new byte[34]);
        }
    }

    @Override
    public Message get(int index) {
        return data[index];
    }

    public Message get() {
        return data[size++];
    }

    @Override
    public void clear() {
        size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Message set(int index,Message m){
        return null;
    }
}
