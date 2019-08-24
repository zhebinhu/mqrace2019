package io.openmessaging;

import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;

/**
 * Created by huzhebin on 2019/08/23.
 */
public class ContextPool {
    private ValueContext[] valueContexts = new ValueContext[24];
    private DataContext[] dataContexts = new DataContext[12];

    private int i = 0;

    private int j= 0;

    public ContextPool() {
        for (int k = 0; k < 24; k++) {
            valueContexts[k] = new ValueContext();
        }
        for (int k = 0; k < 12; k++) {
            valueContexts[k] = new ValueContext();
            dataContexts[k] = new DataContext();
        }
    }

    public synchronized ValueContext getValueContext() {
        ValueContext valueContext = valueContexts[i];
        i++;
        System.out.println("valueContext pool:"+i);
        return valueContext;
    }

    public synchronized DataContext getDataContext() {
        DataContext dataContext = dataContexts[j];
        j++;
        System.out.println("dataContext pool:"+j);
        return dataContext;
    }

}
