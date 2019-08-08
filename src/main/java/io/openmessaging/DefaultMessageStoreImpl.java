package io.openmessaging;

import io.openmessaging.Reader.Reader;

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
    private ConcurrentMap<Thread, Writer> writers = new ConcurrentHashMap<>();

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

    //private Queue queue = new Queue(100);

    //private Set<Thread> threadSet = new HashSet<>();

    @Override
    public void put(Message message) {
        if (!writers.containsKey(Thread.currentThread())) {
            synchronized (this) {
                if (!writers.containsKey(Thread.currentThread())) {
                    writers.put(Thread.currentThread(), new Writer(num.getAndIncrement()));
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

        writers.get(Thread.currentThread()).put(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
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
            //result = forkJoinPool1.submit(new MergeTask(new ArrayList<>(queues.values()), 0, queues.size() - 1, aMin, aMax, tMin, tMax, messagePoolThreadLocal.get())).get();
            //List<List<Message>> messageLists = new ArrayList<>();
            //            for (Queue queue : queues.values()) {
            //                messageLists.add(queue.getMessage(aMin, aMax, tMin, tMax, messagePoolThreadLocal.get()));
            //            }
            //            for (List<Message> messages : messageLists) {
            //                result = merge(result, messages);
            //            }
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
            int t = (int) (o1.fst.getT() - o2.fst.getT());
            if (t == 0) {
                t = (int) (o1.fst.getA() - o2.fst.getA());
            }
            return t;
        });
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
                Pair<Message, Writer> newPair = new Pair<>(newMessage, pair.snd);
                priorityQueue.add(newPair);
            }
        }
        writers.clear();
        System.out.println("init end:" + System.currentTimeMillis());
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        try {
            if (!avg) {
                synchronized (this) {
                    if (!avg) {
                        System.out.println("avg:" + System.currentTimeMillis());
                        avg = true;
                    }
                }
            }
            long starttime = System.currentTimeMillis();
            //int c = count.getAndIncrement();
            //            if (c > 20000) {
            //
            //                if (!end) {
            //                    synchronized (this) {
            //                        if (!end) {
            //                            System.out.println("end:" + System.currentTimeMillis());
            //                            end = true;
            //                        }
            //                    }
            //                }
            //                return 0L;
            //            }
            //Avg result = forkJoinPool2.submit(new AvgTask(new ArrayList<>(queues.values()), 0, queues.size() - 1, aMin, aMax, tMin, tMax)).get();

            //            if (result.getCount() == 0) {
            //                return 0L;
            //            } else {
            //                return result.getTotal() / result.getCount();
            //            }
            long result = reader.avg(aMin, aMax, tMin, tMax);
            long endtime = System.currentTimeMillis();
            System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " getAvgValue: " + (endtime - starttime));
            return result;

        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return 0L;
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

    public class AvgTask extends RecursiveTask<Avg> {
        private int start;

        private int end;

        private List<Queue> queues;

        private long aMin;

        private long aMax;

        private long tMin;

        private long tMax;

        AvgTask(List<Queue> queues, int start, int end, long aMin, long aMax, long tMin, long tMax) {
            this.queues = queues;
            this.start = start;
            this.end = end;
            this.aMin = aMin;
            this.aMax = aMax;
            this.tMin = tMin;
            this.tMax = tMax;
        }

        @Override
        protected Avg compute() {
            Avg result = new Avg();
            try {
                if (start == end) {
                    return queues.get(start).getAvg(aMin, aMax, tMin, tMax);
                } else {
                    int mid = (start + end) / 2;
                    AvgTask leftTask = new AvgTask(queues, start, mid, aMin, aMax, tMin, tMax);
                    AvgTask rightTask = new AvgTask(queues, mid + 1, end, aMin, aMax, tMin, tMax);

                    leftTask.fork();
                    rightTask.fork();

                    Avg leftResult = leftTask.join();
                    Avg rightResult = rightTask.join();

                    result.setCount(leftResult.getCount() + rightResult.getCount());
                    result.setTotal(leftResult.getTotal() + rightResult.getTotal());
                    return result;
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            return result;
        }
    }

    public class MergeTask extends RecursiveTask<List<Message>> {
        private int start;

        private int end;

        private List<Queue> queues;

        private long aMin;

        private long aMax;

        private long tMin;

        private long tMax;

        private MessagePool messagePool;

        MergeTask(List<Queue> queues, int start, int end, long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
            this.queues = queues;
            this.start = start;
            this.end = end;
            this.aMin = aMin;
            this.aMax = aMax;
            this.tMin = tMin;
            this.tMax = tMax;
            this.messagePool = messagePool;
        }

        protected List<Message> compute() {
            List<Message> result = new ArrayList<>();
            try {
                if (start == end) {
                    return queues.get(start).getMessage(aMin, aMax, tMin, tMax, messagePool);
                } else {
                    int mid = (start + end) / 2;
                    MergeTask leftTask = new MergeTask(queues, start, mid, aMin, aMax, tMin, tMax, messagePool);
                    MergeTask rightTask = new MergeTask(queues, mid + 1, end, aMin, aMax, tMin, tMax, messagePool);

                    leftTask.fork();
                    rightTask.fork();

                    List<Message> leftResult = leftTask.join();
                    List<Message> rightResult = rightTask.join();

                    result = merge(leftResult, rightResult);
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            return result;
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
}
