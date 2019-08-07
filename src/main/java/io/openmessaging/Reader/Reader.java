package io.openmessaging.Reader;

import io.openmessaging.Message;
import io.openmessaging.MessagePool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class Reader {
    private TimeReader timeReader;

    private ValueReader valueReader;

    private DataReader dataReader;

    private long msgNum;

    public Reader() {
        timeReader = new TimeReader();
        valueReader = new ValueReader();
        dataReader = new DataReader();
    }

    public void put(Message message) {
        timeReader.put(message);
        valueReader.put(message);
        dataReader.put(message);
        msgNum++;
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
        List<Message> result = new ArrayList<>();
        int offsetA = timeReader.getOffset((int) tMin);
        System.out.println("offsetA:" + offsetA);
        while (offsetA < msgNum) {
            System.out.println("offsetA:" + offsetA);
            long time = timeReader.get(offsetA);
            if (time > tMax) {
                return result;
            }
            System.out.println("time:"+time);
            long value = valueReader.get(offsetA) + time;
            System.out.println("value:"+value);
            if (value > aMax || value < aMin) {
                offsetA++;
                continue;
            }
            Message message = messagePool.get();
            message.setT(time);
            message.setA(value);
            dataReader.getData(offsetA, message);
            result.add(message);
            offsetA++;
        }
        return result;
    }
}
