package io.openmessaging;

import io.openmessaging.Context.ValueContext;

/**
 * Created by huzhebin on 2019/08/23.
 */
public class ContextPool {
    public ValueContext[] valueContexts = new ValueContext[24];

    private int i;

    public ContextPool() {
        for (int i = 0; i < 24; i++) {
            valueContexts[i] = new ValueContext();
        }
    }

    public synchronized ValueContext getValueContext() {
        System.out.println("pool " + i);
        return valueContexts[i++];
    }

}
