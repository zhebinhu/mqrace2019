package io.openmessaging;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    /**
     * 一个线程对应一个队列
     */
    private ConcurrentMap<Thread, Queue> queues = new ConcurrentHashMap<>();

    private AtomicInteger num = new AtomicInteger(0);

    private volatile ForkJoinPool forkJoinPool;

    private volatile ForkJoinPool avgJoinPool;

    private Semaphore semaphore = new Semaphore(2);

    private Reader reader = new Reader(100);

    private volatile boolean copied = false;

    private long putStartTime = 0;

    private long startTime = 0;

    @Override
    public void put(Message message) {
        if (!queues.containsKey(Thread.currentThread())) {
            synchronized (this) {
                if (!queues.containsKey(Thread.currentThread())) {
                    queues.put(Thread.currentThread(), new Queue(num.getAndIncrement()));
                }
            }
        }
        if (putStartTime == 0) {
            synchronized (this) {
                if (startTime == 0) {
                    putStartTime = System.currentTimeMillis();
                    System.out.println("put start at:" + putStartTime);
                }
            }
        }
        queues.get(Thread.currentThread()).put(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        //        if (forkJoinPool == null) {
        //            synchronized (this) {
        //                if (forkJoinPool == null) {
        //                    forkJoinPool = new ForkJoinPool();
        //                }
        //            }
        //        }
        //        try {
        //            semaphore.acquire();
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }
        //        copy();
        if (startTime == 0) {
            synchronized (this) {
                if (startTime == 0) {
                    startTime = System.currentTimeMillis();
                }
            }
        }
        //System.out.println("1 " + aMin + " " + aMax + " " + tMin + " " + tMax + " " + (System.currentTimeMillis() - startTime));
        //        List<Message> result = forkJoinPool.invoke(new MergeTask(new ArrayList<>(queues.values()), 0, queues.size() - 1, aMin, aMax, tMin, tMax));
        //        semaphore.release();
        //        return result;
        if (!copied) {
            synchronized (this) {
                if (!copied) {
                    //copy();
                    copied = true;
                }
            }
        }

        return reader.getMessage(aMin, aMax, tMin, tMax);
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return (int) reader.getMessage(aMin, aMax, tMin, tMax).stream().mapToLong(Message::getA).average().getAsDouble();
    }

}
