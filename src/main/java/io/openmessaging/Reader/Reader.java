package io.openmessaging.Reader;

import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;
import io.openmessaging.MessagePool;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class Reader {
    private TimeReader timeReader;

    private ValueReader valueReader;

    private DataReader dataReader;

    private long msgNum;

    private ThreadLocal<TimeContext> timeContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<ValueContext> valueContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<DataContext> dataContextThreadLocal = new ThreadLocal<>();

    AtomicLong one = new AtomicLong();

    AtomicLong two = new AtomicLong();

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
        if (timeContextThreadLocal.get() == null) {
            timeContextThreadLocal.set(new TimeContext());
        }
        TimeContext timeContext = timeContextThreadLocal.get();
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(new ValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        if (dataContextThreadLocal.get() == null) {
            dataContextThreadLocal.set(new DataContext());
        }
        DataContext dataContext = dataContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        while (offsetA < msgNum) {
            long time = timeReader.get(offsetA, timeContext);
            if (time > tMax) {
                return result;
            }
            long value = valueReader.get(offsetA, valueContext);
            if (value > aMax || value < aMin) {
                offsetA++;
                continue;
            }
            Message message = messagePool.get();
            message.setT(time);
            message.setA(value);
            dataReader.getData(offsetA, message, dataContext);
            result.add(message);
            offsetA++;
        }
        return result;
    }

    public long avg(long aMin, long aMax, long tMin, long tMax) {
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(new ValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        return valueReader.avg(offsetA,offsetB,aMin,aMax,valueContext);
    }
}
