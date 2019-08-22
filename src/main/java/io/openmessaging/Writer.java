package io.openmessaging;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huzhebin on 2019/08/07.
 */
public class Writer {
    private int num;

    //private ByteBuffer buffer = ByteBuffer.allocate(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    private FileChannel fileChannel;

    private Lock lock = new ReentrantLock();

    private Condition notEmpty = lock.newCondition();

    private Condition notFull = lock.newCondition();

    private int cap = Constants.MESSAGE_NUM * 16;

    private int size = 0;

    private int pWrite = 0;

    private int pRead = 0;

    private Thread thread;

    Message[] messages = new Message[cap];

    public Writer(Thread thread) {
        this.thread = thread;
    }

    public void put(Message message) {
        lock.lock();
        if (size == cap) {
            try {
                notFull.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        messages[pWrite] = message;
        size++;
        pWrite = (pWrite + 1) % cap;
        notEmpty.signal();
        lock.unlock();
    }

    public Message get() {
        lock.lock();
        while (size == 0) {
            System.out.println("get is empty:" + System.currentTimeMillis());
            if (!thread.isAlive()) {
                return null;
            }
            try {
                notEmpty.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Message result = messages[pRead];
        pRead = (pRead + 1) % cap;
        size--;
        if (size == 0) {
            notFull.signal();
        }
        lock.unlock();
        return result;
    }
}