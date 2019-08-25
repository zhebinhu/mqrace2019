package io.openmessaging;

import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;

/**
 * Created by huzhebin on 2019/08/23.
 */
public class ContextPool {
    private ValueContext[] valueContexts = new ValueContext[12];

    private int i = 0;

    public ContextPool() {
        for (int k = 0; k < 12; k++) {
            valueContexts[k] = new ValueContext();
        }
    }

    public synchronized ValueContext getValueContext() {
        ValueContext valueContext = valueContexts[i];
        i = (i + 1) % 12;
        return valueContext;
    }

}
