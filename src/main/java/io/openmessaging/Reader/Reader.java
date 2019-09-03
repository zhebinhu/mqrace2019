package io.openmessaging.Reader;

import io.openmessaging.*;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.ValueContext;

import java.util.List;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class Reader {
    private TimeReader timeReader;

    private ValueReader valueReader;

    private DataReader dataReader;

    private ContextPool contextPool;

    private ThreadLocal<TimeContext> timeContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<ValueContext> valueContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<DataContext> dataContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<MessageList> messageListThreadLocal = new ThreadLocal<>();

    public Reader(int num) {
        timeReader = new TimeReader();
        valueReader = new ValueReader(num);
        dataReader = new DataReader(num);
    }

    public void put(Message message) {
        timeReader.put(message);
        valueReader.put(message);
        dataReader.put(message);
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        if (messageListThreadLocal.get() == null) {
            messageListThreadLocal.set(new MessageList());
        }
        MessageList result = messageListThreadLocal.get();
        result.clear();
        if (timeContextThreadLocal.get() == null) {
            timeContextThreadLocal.set(new TimeContext());
        }
        TimeContext timeContext = timeContextThreadLocal.get();
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(contextPool.getValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        if (dataContextThreadLocal.get() == null) {
            dataContextThreadLocal.set(new DataContext());
        }
        DataContext dataContext = dataContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        valueReader.updateContext(offsetA, offsetB, valueContext);
        //System.out.println("tMin:" + tMin + " tMax:" + tMax + " minblock:" + getBlock(aMin) + " maxblock:" + getBlock(aMax) + " offsetA:" + offsetA + " offsetB:" + offsetB + " " + valueContext.buffer);
        while (offsetA < offsetB) {
            long time = timeReader.get(offsetA, timeContext);
            long value = valueReader.get(offsetA, valueContext);
            if (value > aMax || value < aMin) {
                offsetA++;
                continue;
            }
            Message message = result.get();
            message.setT(time);
            message.setA(value);
            dataReader.getData(offsetA, message, dataContext);
            offsetA++;
        }
        return result;
    }

    public Avg avg(long aMin, long aMax, long tMin, long tMax) {
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(contextPool.getValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        return valueReader.avg(offsetA, offsetB, aMin, aMax, valueContext);
    }

    public void init() {
        contextPool = new ContextPool();
        valueReader.init();
        dataReader.init();
        timeReader.init();
    }
}
