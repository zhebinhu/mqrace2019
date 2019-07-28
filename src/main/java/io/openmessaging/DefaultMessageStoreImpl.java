package io.openmessaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
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

    private Semaphore semaphore = new Semaphore(2);

    private volatile boolean put = false;

    private volatile boolean get = false;

    @Override
    public void put(Message message) {
        if (!queues.containsKey(Thread.currentThread())) {
            synchronized (this) {
                if (!queues.containsKey(Thread.currentThread())) {
                    queues.put(Thread.currentThread(), new Queue(num.getAndIncrement()));
                }
            }
        }
        if (!put) {
            synchronized (this) {
                if (!put) {
                    System.out.println("put:" + System.currentTimeMillis());
                    put = true;
                }
            }
        }
        queues.get(Thread.currentThread()).put(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (!get) {
            synchronized (this) {
                if (!get) {
                    System.out.println("get:" + System.currentTimeMillis());
                    for (Queue queue : queues.values()) {
                        System.out.println("indexmap size:" + queue.indexMap.size());
                    }
                    get = true;
                }
            }
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long starttime = System.currentTimeMillis();
        List<List<Message>> messageLists = new ArrayList<>();
        for (Queue queue : queues.values()) {
            messageLists.add(queue.getMessage(aMin, aMax, tMin, tMax));
        }
        long middletime = System.currentTimeMillis();
        System.out.println("getMessage cost:" + (middletime - starttime));
        List<Message> result = new ArrayList<>();
        for (List<Message> list : messageLists) {
            result = merge(result, list);
        }
        semaphore.release();
        System.out.println("mergeMessage cost:" + (System.currentTimeMillis() - middletime));
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return (long) queues.values().stream().parallel().map(queue -> queue.getMessage(aMin, aMax, tMin, tMax)).flatMap(Collection::stream).mapToLong(Message::getA).average().getAsDouble();
    }

    private List<Message> merge(List<Message> a, List<Message> b) {
        List<Message> result = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < a.size() && j < b.size()) {
            if (a.get(i).getT() <= b.get(j).getT()) {
                result.add(a.get(i++));
            } else {
                result.add(b.get(j++));
            }
        }
        while (i < a.size()) {
            result.add(a.get(i++));
        }
        while (j < b.size()) {
            result.add(b.get(j++));
        }
        return result;
    }
}
