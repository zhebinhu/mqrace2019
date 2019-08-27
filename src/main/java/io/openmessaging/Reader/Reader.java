package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.ContextPool;
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

    private ContextPool contextPool;

    private long msgNum;

    private ThreadLocal<TimeContext> timeContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<ValueContext> valueContextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<DataContext> dataContextThreadLocal = new ThreadLocal<>();

    public Reader() {
        timeReader = new TimeReader();
        valueReader = new ValueReader();
        dataReader = new DataReader();
        contextPool = new ContextPool();
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
        dataReader.updateContext(offsetA, offsetB, dataContext);
        for (int i = 0; i < (offsetB - offsetA); i++) {
            long time = timeReader.get(offsetA + i, timeContext);
            long value = valueContext.buffer.getLong();
            if (value > aMax || value < aMin) {
                continue;
            }
            Message message = messagePool.get();
            message.setT(time);
            message.setA(value);
            dataContext.buffer.position(i * Constants.DATA_SIZE);
            dataContext.buffer.get(message.getBody());
            result.add(message);
        }
        return result;
    }

    public long avg(long aMin, long aMax, long tMin, long tMax) {
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(contextPool.getValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        return valueReader.avg(offsetA, offsetB, aMin, aMax, valueContext);
    }

    public void init() {
        valueReader.init();
        dataReader.init();
        timeReader.init();
    }
}
