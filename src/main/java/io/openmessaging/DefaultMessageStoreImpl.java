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

    private ConcurrentMap<Thread, Writer> writers = new ConcurrentHashMap<>();

    private ConcurrentMap<Thread, BlockingQueue<Message>> queues = new ConcurrentHashMap<>();

    private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 10, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

    private Reader reader;

    private AtomicInteger num = new AtomicInteger(0);

    private volatile boolean put = false;

    private volatile boolean get = false;

    private volatile boolean avg = false;

    private volatile boolean end = false;

    private volatile boolean merged = false;

    private ThreadLocal<MessagePool> messagePoolThreadLocal = new ThreadLocal<>();

    private AtomicInteger count = new AtomicInteger(0);

    private volatile boolean inited = false;

    private Future future;

    //private Set<Thread> threadSet = new HashSet<>();

    @Override
    public void put(Message message) {
        if (!queues.containsKey(Thread.currentThread())) {
            synchronized (this) {
                if (!queues.containsKey(Thread.currentThread())) {
                    queues.put(Thread.currentThread(), new LinkedBlockingQueue<>(100000));
                }
            }
        }
        if (!put) {
            synchronized (this) {
                if (!put) {
                    System.out.println("put:" + System.currentTimeMillis());
                    future = threadPoolExecutor.submit(() -> {
                        try {
                            System.out.println("init start:" + System.currentTimeMillis());
                            reader = new Reader();
                            PriorityQueue<Pair<Message, Pair<BlockingQueue<Message>, Thread>>> priorityQueue = new PriorityQueue<>((o1, o2) -> {
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
                            updateDatePriorityQueue(priorityQueue);
                            while (!priorityQueue.isEmpty()) {
                                Pair<Message, Pair<BlockingQueue<Message>, Thread>> pair = priorityQueue.poll();
                                reader.put(pair.fst);
                                Message newMessage = null;
                                while (newMessage == null && (pair.snd.snd.isAlive() || !pair.snd.fst.isEmpty())) {
                                    newMessage = pair.snd.fst.poll(1, TimeUnit.SECONDS);

                                }
                                if (newMessage != null) {
                                    pair.fst = newMessage;
                                    priorityQueue.add(pair);
                                }
                                if (priorityQueue.isEmpty()) {
                                    Thread.sleep(1000);
                                    updateDatePriorityQueue(priorityQueue);
                                }
                            }
                            queues.clear();
                            System.out.println("init end:" + System.currentTimeMillis());
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                        }
                    });
                    put = true;
                }
            }
        }
        try {
            queues.get(Thread.currentThread()).put(message);
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    private void updateDatePriorityQueue(PriorityQueue<Pair<Message, Pair<BlockingQueue<Message>, Thread>>> priorityQueue) throws Exception {
        for (Map.Entry<Thread, BlockingQueue<Message>> entry : queues.entrySet()) {
            Message m = null;
            while (m == null && (entry.getKey().isAlive() || !entry.getValue().isEmpty())) {
                m = entry.getValue().poll(1, TimeUnit.SECONDS);
            }
            if (m != null) {
                Pair<Message, Pair<BlockingQueue<Message>, Thread>> pair = new Pair<>(m, new Pair<>(entry.getValue(), entry.getKey()));
                priorityQueue.add(pair);
            }
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                    inited = true;
                }
            }
        }
        try {
            if (!get) {
                synchronized (this) {
                    if (!get) {
                        System.out.println("get:" + System.currentTimeMillis());
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

    //    private void init() {
    //        System.out.println("init start:" + System.currentTimeMillis());
    //        reader = new Reader();
    //        PriorityQueue<Pair<Message, Writer>> priorityQueue = new PriorityQueue<>((o1, o2) -> {
    //            long t = o1.fst.getT() - o2.fst.getT();
    //            if (t == 0) {
    //                t = o1.fst.getA() - o2.fst.getA();
    //            }
    //            if (t == 0) {
    //                return 0;
    //            }
    //            if (t > 0) {
    //                return 1;
    //            }
    //            return -1;
    //
    //        });
    //        for (Writer writer : writers.values()) {
    //            Message message = writer.get();
    //            Pair<Message, Writer> pair = new Pair<>(message, writer);
    //            priorityQueue.add(pair);
    //        }
    //        while (!priorityQueue.isEmpty()) {
    //            Pair<Message, Writer> pair = priorityQueue.poll();
    //            reader.put(pair.fst);
    //            Message newMessage = pair.snd.get();
    //            if (newMessage != null) {
    //                pair.fst = newMessage;
    //                priorityQueue.add(pair);
    //            }
    //        }
    //        writers.clear();
    //        System.out.println("init end:" + System.currentTimeMillis());
    //    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        //            if (!avg) {
        //                synchronized (this) {
        //                    if (!avg) {
        //                        System.out.println("avg:" + System.currentTimeMillis());
        //                        avg = true;
        //                    }
        //                }
        //            }
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
