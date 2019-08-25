package io.openmessaging.Context;

import io.openmessaging.ContextPool;
import io.openmessaging.MessageList;

/**
 * Created by huzhebin on 2019/08/25.
 */
public class Context {
    public TimeContext timeContext = new TimeContext();
    public DataContext dataContext = new DataContext();
    public ValueContext valueContext = ContextPool.getValueContext();
    public MessageList messageList = new MessageList();

}
