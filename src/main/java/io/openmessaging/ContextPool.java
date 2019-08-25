package io.openmessaging;

import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;

/**
 * Created by huzhebin on 2019/08/23.
 */
public class ContextPool {
    private static ValueContext[] valueContexts = new ValueContext[12];
//
//    private DataContext[] dataContexts = new DataContext[12];
//
    private static int i = 0;
//
//    private int j = 0;
//
    static  {
        for (int k = 0; k < 12; k++) {
            valueContexts[k] = new ValueContext();
        }
    }
//
    public static synchronized ValueContext getValueContext() {
        ValueContext valueContext = valueContexts[i];
        i = (i + 1) % 12;
        return valueContext;
    }
//
//    public synchronized DataContext getDataContext() {
//        DataContext dataContext = dataContexts[j];
//        j = (j + 1) % 12;
//        return dataContext;
//    }

}
