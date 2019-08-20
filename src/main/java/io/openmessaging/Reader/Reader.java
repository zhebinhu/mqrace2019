package io.openmessaging.Reader;

import io.openmessaging.AvgResult;
import io.openmessaging.Constants;
import io.openmessaging.Context.TimeContext;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;
import io.openmessaging.MessagePool;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
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

    public Reader(int num) {
        valueReader = new ValueReader(num);
        dataReader = new DataReader(num);
        timeReader = new TimeReader(valueReader, dataReader);
    }

    public void put(Message message) {
        timeReader.put(message);
        valueReader.put(message);
        dataReader.put(message);
        msgNum++;
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {

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
        return timeReader.getMessage(aMin, aMax, tMin, tMax, messagePool, timeContext, valueContext, dataContext);

    }

    public AvgResult avg(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        int count = 0;
        if (valueContextThreadLocal.get() == null) {
            valueContextThreadLocal.set(new ValueContext());
        }
        ValueContext valueContext = valueContextThreadLocal.get();
        int offsetA = timeReader.getOffset(tMin);
        int offsetB = timeReader.getOffset(tMax + 1);
        while (offsetA < offsetB) {
            long value = valueReader.get(offsetA, valueContext);
            if (value > aMax || value < aMin) {
                offsetA++;
                continue;
            }
            sum += value;
            count++;
            offsetA++;
        }
        return new AvgResult(sum, count);
    }
}
