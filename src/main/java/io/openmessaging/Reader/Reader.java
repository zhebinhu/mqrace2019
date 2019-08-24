package io.openmessaging.Reader;

import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.ContextPool;
import io.openmessaging.Message;
import io.openmessaging.MessagePool;

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
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
        if (timeContextThreadLocal.get() == null) {
            timeContextThreadLocal.set(new TimeContext());
        }
        TimeContext timeContext = timeContextThreadLocal.get();
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(contextPool.getValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        if (dataContextThreadLocal.get() == null) {
            dataContextThreadLocal.set(contextPool.getDataContext());
        }
        DataContext dataContext = dataContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        valueReader.pre(offsetA,offsetB,valueContext);
        dataReader.pre(offsetA,offsetB,dataContext);
        return timeReader.getMessage(offsetA,offsetB,aMin,aMax,timeContext,valueContext,dataContext,messagePool);
    }

    public long avg(long aMin, long aMax, long tMin, long tMax) {
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(contextPool.getValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        return valueReader.avg(offsetA,offsetB,aMin,aMax,valueContext);
    }
}
