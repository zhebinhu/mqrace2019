package io.openmessaging;

import io.openmessaging.Reader.Reader;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private HashMap<Thread, Writer> writers = new HashMap<>();

    private Reader[] readers;

    private volatile boolean put = false;

    private volatile boolean get = false;

    private volatile boolean avg = false;

    private volatile boolean end = false;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        return thread;
    });

    private Future future;

    private long[] barriers = new long[Constants.VALUE_BLOCKS - 1];

    private AtomicInteger count = new AtomicInteger(0);


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
        try {
            if (!get) {
                synchronized (this) {
                    if (!get) {
                        System.out.println("get:" + System.currentTimeMillis());
                        future.get();
                        for (Reader reader : readers) {
                            reader.init();
                        }
                        get = true;
                    }
                }
            }
            for (int i = getBlock(aMin); i <= getBlock(aMax); i++) {
                //result = reader.get(aMin, aMax, tMin, tMax);
                result = merge(result, readers[i].get(aMin, aMax, tMin, tMax));
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return result;
    }

    private void init() {
        try {
            System.out.println("init start" + System.currentTimeMillis());
            List<Message> cache = new ArrayList<>();
            readers = new Reader[Constants.VALUE_BLOCKS];
            for (int i = 0; i < Constants.VALUE_BLOCKS; i++) {
                readers[i] = new Reader(i);
            }
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
            while (!priorityQueue.isEmpty() && (cache.size() < Constants.INITIAL_FLOW)) {
                Pair<Message, Writer> pair = priorityQueue.poll();
                cache.add(pair.fst);
                Message newMessage = pair.snd.get();
                if (newMessage != null) {
                    pair.fst = newMessage;
                    priorityQueue.add(pair);
                }
            }
            List<Message> tmp = new ArrayList<>(cache);
            tmp.sort((o1, o2) -> {
                long t = o1.getA() - o2.getA();
                if (t == 0) {
                    return 0;
                }
                if (t > 0) {
                    return 1;
                }
                return -1;
            });
            for (int i = 0; i < Constants.VALUE_BLOCKS - 1; i++) {
                barriers[i] = tmp.get(Constants.INITIAL_FLOW / Constants.VALUE_BLOCKS * (i + 1)).getA();
            }
            System.out.println("barriers:" + Arrays.toString(barriers));
            for (Message message : cache) {
                readers[getBlock(message.getA())].put(message);
            }
            System.out.println("barriers end" + System.currentTimeMillis());
            tmp = null;
            cache = null;
            System.gc();
            while (!priorityQueue.isEmpty()) {
                Pair<Message, Writer> pair = priorityQueue.poll();
                readers[getBlock(pair.fst.getA())].put(pair.fst);
                Message newMessage = pair.snd.get();
                if (newMessage != null) {
                    pair.fst = newMessage;
                    priorityQueue.add(pair);
                }
            }
            writers.clear();
            System.out.println("init end" + System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.exit(-1);
        }
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

        if (count.getAndIncrement() == 35000) {
            if (!end) {
                synchronized (this) {
                    if (!end) {
                        System.out.println("end:" + System.currentTimeMillis());
                        end = true;
                        return 0L;
                    }
                }
            }
        }
        //                    long result = reader.avg(aMin, aMax, tMin, tMax);
        //                    long endtime = System.currentTimeMillis();
        //                    System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " getAvgValue: " + (endtime - starttime));
        //System.out.println("memory:" + memoryLoad());

        Avg avg = new Avg();
        for (int i = getBlock(aMin); i <= getBlock(aMax); i++) {
            //result = reader.get(aMin, aMax, tMin, tMax);
            Avg tmp = readers[i].avg(aMin, aMax, tMin, tMax);
            avg.sum += tmp.sum;
            avg.count += tmp.count;
        }
        return avg.count == 0 ? 0 : avg.sum / avg.count;

        //return 0L;

    }

    private int getBlock(long value) {
        for (int i = Constants.VALUE_BLOCKS - 1; i > 0; i--) {
            if (value > barriers[i - 1]) {
                return i;
            }
        }
        return 0;
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
