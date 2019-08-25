package io.openmessaging;

import java.util.*;

/**
 * Created by huzhebin on 2019/08/25.
 */
public class MessageList extends AbstractList<Message> {
    private Message[] data = new Message[80000];
    private int size = 0;

    public MessageList(){
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
    public int size() {
        return size;
    }

    @Override
    public boolean add(Message m){
        data[size] = m;
        size++;
        return true;
    }

    @Override
    public void clear(){
        size = 0;
    }


}
