package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class Writer {

    private final static int cap = Constants.WRITER_CAP;

    private AtomicInteger size = new AtomicInteger(0);

    private int pWrite = 0;

    private int pRead = 0;

    private Thread thread;

    Message[] messages = new Message[cap];

    public Writer(Thread thread) {
        this.thread = thread;
    }

    public void put(Message message) {
        while (size.intValue() == cap) {
            LockSupport.park();
        }
        messages[pWrite] = message;
        size.getAndIncrement();
        pWrite = (pWrite + 1) % cap;
    }

    public Message get() {
        while (size.intValue() == 0) {
            if (!thread.isAlive()) {
                return null;
            }
        }
        Message result = messages[pRead];
        pRead = (pRead + 1) % cap;
        if (size.decrementAndGet() == 0) {
            LockSupport.unpark(thread);
        }
        return result;
    }
}