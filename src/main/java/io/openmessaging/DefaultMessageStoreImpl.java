package io.openmessaging;

import com.sun.management.OperatingSystemMXBean;
import io.openmessaging.Reader.Reader;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private HashMap<Thread, Writer> writers = new HashMap<>();

    private Reader reader;

    private AtomicInteger num = new AtomicInteger(0);

    private volatile boolean put = false;

    private volatile boolean get = false;

    private volatile boolean avg = false;

    private volatile boolean end = false;

    private volatile boolean merged = false;

    //private ForkJoinPool forkJoinPool1 = new ForkJoinPool(20);

    //private ForkJoinPool forkJoinPool2 = new ForkJoinPool(20);

    private ThreadLocal<MessagePool> messagePoolThreadLocal = new ThreadLocal<>();

    private AtomicInteger count = new AtomicInteger(0);

    private volatile boolean inited = false;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        thread.setDaemon(true);
        return thread;
    });

    private Future future;

    //private Set<Thread> threadSet = new HashSet<>();

    @Override
    public void put(Message message) {
        if (!writers.containsKey(Thread.currentThread())) {
            synchronized (this) {
                writers.put(Thread.currentThread(), new Writer(Thread.currentThread()));
            }
        }
        if (!put) {
            synchronized (this) {
                if (!put) {
                    System.out.println("put:" + System.currentTimeMillis());
                    future = executorService.submit(this::init);
                    put = true;
                }
            }
        }
        writers.get(Thread.currentThread()).put(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();
//        if (!inited) {
//            synchronized (this) {
//                if (!inited) {
//                    //init();
//                    inited = true;
//                }
//            }
//        }
        try {
            if (!get) {
                synchronized (this) {
                    if (!get) {
                        System.out.println("get:" + System.currentTimeMillis());
                        future.get();
                        get = true;
                    }
                }
            }
            if (messagePoolThreadLocal.get() == null) {
                messagePoolThreadLocal.set(new MessagePool());
            }
            //long starttime = System.currentTimeMillis();
            result = reader.get(aMin, aMax, tMin, tMax, messagePoolThreadLocal.get());
            //long endtime = System.currentTimeMillis();
            //System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " size: " + (result.size()) + " getMessage: " + (endtime - starttime));
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return result;
    }

    private void init() {
        System.out.println("init start:" + System.currentTimeMillis());
        reader = new Reader();
        PriorityQueue<Pair<Message, Writer>> priorityQueue = new PriorityQueue<>((o1, o2) -> {
            long t = o1.fst.getT() - o2.fst.getT();
            if (t == 0) {
                t = o1.fst.getA() - o2.fst.getA();
            }
            if (t == 0) {
                return 0;
            }
            if (t > 0) {
                return 1;
            }
            return -1;

        });
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        for (Writer writer : writers.values()) {
            Message message = writer.get();
            Pair<Message, Writer> pair = new Pair<>(message, writer);
            priorityQueue.add(pair);
        }
        while (!priorityQueue.isEmpty()) {
            Pair<Message, Writer> pair = priorityQueue.poll();
            reader.put(pair.fst);
            Message newMessage = pair.snd.get();
            if (newMessage != null) {
                pair.fst = newMessage;
                priorityQueue.add(pair);
            }
        }
        writers.clear();
        System.out.println("init end:" + System.currentTimeMillis());
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (!avg) {
            synchronized (this) {
                if (!avg) {
                    System.out.println("avg:" + System.currentTimeMillis());
                    avg = true;
                }
            }
        }
        //            long starttime = System.currentTimeMillis();
        //            if (count.getAndIncrement() == 28000) {
        //                if (!end) {
        //                    synchronized (this) {
        //                        if (!end) {
        //                            System.out.println("end:" + System.currentTimeMillis());
        //                            end = true;
        //                            return 0L;
        //                        }
        //                    }
        //                }
        //            }
        //            long result = reader.avg(aMin, aMax, tMin, tMax);
        //            long endtime = System.currentTimeMillis();
        //            System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " getAvgValue: " + (endtime - starttime));
        //System.out.println("memory:" + memoryLoad());
        return reader.avg(aMin, aMax, tMin, tMax);
        //return 0L;

    }
}
