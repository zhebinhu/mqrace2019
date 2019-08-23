package io.openmessaging;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

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
            //System.out.println("put full");
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
            //System.out.println("get empty");
        }
        Message result = messages[pRead];
        pRead = (pRead + 1) % cap;
        size.getAndDecrement();
        if (size.intValue() == 0) {
            //System.out.println("unpark");
            LockSupport.unpark(thread);
        }
        return result;
    }
}