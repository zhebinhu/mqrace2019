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

    private ConcurrentMap<Thread, Reader> readers = new ConcurrentHashMap<>();

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

    private ForkJoinPool forkJoinPool = new ForkJoinPool(1);

    //private Set<Thread> threadSet = new HashSet<>();

    @Override
    public void put(Message message) {
        if (!readers.containsKey(Thread.currentThread())) {
            synchronized (this) {
                if (!readers.containsKey(Thread.currentThread())) {
                    readers.put(Thread.currentThread(), new Reader(count.getAndIncrement()));
                }
            }
        }
        if (!put) {
            synchronized (this) {
                System.out.println(System.currentTimeMillis());
                put = true;
            }
        }
        readers.get(Thread.currentThread()).put(message);
    }

    private void updateDatePriorityQueue
            (PriorityQueue<Pair<Message, Pair<BlockingQueue<Message>, Thread>>> priorityQueue) throws Exception {
        for (Map.Entry<Thread, BlockingQueue<Message>> entry : queues.entrySet()) {
            Message m = null;
            if (entry.getKey().isAlive() || !entry.getValue().isEmpty()) {
                m = entry.getValue().poll(1, TimeUnit.SECONDS);
            }
            if (m != null) {
                Pair<Message, Pair<BlockingQueue<Message>, Thread>> pair = new Pair<>(m, new Pair<>(entry.getValue(), entry.getKey()));
                priorityQueue.add(pair);
            }
        }
    }

    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (!get) {
            synchronized (this) {
                if (!get) {
                    System.out.println("get:" + System.currentTimeMillis());
                    get = true;
                }
            }
        }
        try {
            if (messagePoolThreadLocal.get() == null) {
                messagePoolThreadLocal.set(new MessagePool());
            }
            System.out.println("get");
            return forkJoinPool.submit(new MergeTask(new ArrayList<>(readers.values()), 0, readers.size() - 1, aMin, aMax, tMin, tMax, messagePoolThreadLocal.get())).get();
            //result = reader.get(aMin, aMax, tMin, tMax, messagePoolThreadLocal.get());
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }

        return null;
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
        try {
            AvgResult avgResult = forkJoinPool.submit(new AvgTask(new ArrayList<>(readers.values()), 0, readers.size() - 1, aMin, aMax, tMin, tMax)).get();
            return avgResult.count == 0 ? 0 : avgResult.sum / avgResult.count;
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return 0L;
        //return 0L;

    }

    public class AvgTask extends RecursiveTask<AvgResult> {
        private int start;

        private int end;

        private List<Reader> readers;

        private long aMin;

        private long aMax;

        private long tMin;

        private long tMax;

        AvgTask(List<Reader> readers, int start, int end, long aMin, long aMax, long tMin, long tMax) {
            this.readers = readers;
            this.start = start;
            this.end = end;
            this.aMin = aMin;
            this.aMax = aMax;
            this.tMin = tMin;
            this.tMax = tMax;
        }

        @Override
        protected AvgResult compute() {
            try {
                if (start == end) {
                    return readers.get(start).avg(aMin, aMax, tMin, tMax);
                } else {
                    int mid = (start + end) / 2;
                    AvgTask leftTask = new AvgTask(readers, start, mid, aMin, aMax, tMin, tMax);
                    AvgTask rightTask = new AvgTask(readers, mid + 1, end, aMin, aMax, tMin, tMax);

                    leftTask.fork();
                    rightTask.fork();

                    AvgResult leftResult = leftTask.join();
                    AvgResult rightResult = rightTask.join();

                    return new AvgResult(leftResult.sum + rightResult.sum, leftResult.count + rightResult.count);
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            return null;
        }
    }

    public class MergeTask extends RecursiveTask<List<Message>> {
        private int start;

        private int end;

        private List<Reader> readers;

        private long aMin;

        private long aMax;

        private long tMin;

        private long tMax;

        private MessagePool messagePool;

        MergeTask(List<Reader> readers, int start, int end, long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
            this.readers = readers;
            this.start = start;
            this.end = end;
            this.aMin = aMin;
            this.aMax = aMax;
            this.tMin = tMin;
            this.tMax = tMax;
            this.messagePool = messagePool;
        }

        @Override
        protected List<Message> compute() {
            List<Message> result = new ArrayList<>();
            try {
                if (start == end) {
                    return readers.get(start).get(aMin, aMax, tMin, tMax, messagePool);
                } else {
                    int mid = (start + end) / 2;
                    MergeTask leftTask = new MergeTask(readers, start, mid, aMin, aMax, tMin, tMax, messagePool);
                    MergeTask rightTask = new MergeTask(readers, mid + 1, end, aMin, aMax, tMin, tMax, messagePool);

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
