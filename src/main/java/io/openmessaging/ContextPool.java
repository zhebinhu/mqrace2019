package io.openmessaging;

import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huzhebin on 2019/08/23.
 */
public class ContextPool {
    private ValueContext[] valueContexts = new ValueContext[24];
    private DataContext[] dataContexts = new DataContext[12];

    private AtomicInteger i = new AtomicInteger(0);

    private AtomicInteger j= new AtomicInteger(0);

    public ContextPool() {
        for (int k = 0; k < 24; k++) {
            valueContexts[k] = new ValueContext();
        }
        for (int k = 0; k < 12; k++) {
            valueContexts[k] = new ValueContext();
            dataContexts[k] = new DataContext();
        }
    }

    public ValueContext getValueContext() {
        return valueContexts[i.getAndIncrement()];
    }

    public DataContext getDataContext() {
        return dataContexts[j.getAndIncrement()];
    }

}
