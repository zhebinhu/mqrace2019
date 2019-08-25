package io.openmessaging.Reader;

import io.openmessaging.*;
import io.openmessaging.Context.Context;
import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;

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

    private ThreadLocal<Context> contextThreadLocal = new ThreadLocal<>();

    private ThreadLocal<ValueContext> valueContextThreadLocal = new ThreadLocal<>();

    public Reader() {
        timeReader = new TimeReader();
        valueReader = new ValueReader();
        dataReader = new DataReader();
    }

    public void put(Message message) {
        timeReader.put(message);
        valueReader.put(message);
        dataReader.put(message);
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        if(contextThreadLocal.get()==null){
            contextThreadLocal.set(new Context());
        }
        Context context = contextThreadLocal.get();
        context.messageList.clear();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax+1);
        valueReader.updateContext(offsetA,offsetB,context.valueContext);
        while (offsetA < offsetB) {
            long time = timeReader.get(offsetA, context.timeContext);
            long value = context.valueContext.buffer.getLong();
            if (value <= aMax && value >= aMin) {
                Message message = context.messageList.get();
                message.setT(time);
                message.setA(value);
                dataReader.getData(offsetA, message, context.dataContext);
            }
            offsetA++;
        }
        return context.messageList;
    }

    public long avg(long aMin, long aMax, long tMin, long tMax) {
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(ContextPool.getValueContext());
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
