package io.openmessaging.Reader;

import io.openmessaging.Message;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class Reader {
    private int num;

    private TimeReader timeReader;

    private ValueReader valueReader;

    private DataReader dataReader;

    public Reader(int num) {
        timeReader = new TimeReader();
        valueReader = new ValueReader();
        dataReader = new DataReader();
    }

    public void put(Message message){
        timeReader.put(message);
        valueReader.put(message);
        dataReader.put(message);
    }

    public void get() {
        timeReader.get();
        valueReader.get();
    }
}
