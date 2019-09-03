package io.openmessaging;

import io.openmessaging.Reader.Reader;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private HashMap<Thread, Writer> writers = new HashMap<>();

    private Reader[] readers;

    private volatile boolean put = false;

    private volatile boolean get = false;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        return thread;
    });

    private Future future;

    private long[] barriers = new long[4];

    private int msgNum = 0;

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
            long starttime = System.currentTimeMillis();
            for (int i = getBlock(aMin); i <= getBlock(aMax); i++) {
                //result = reader.get(aMin, aMax, tMin, tMax);
                result = merge(result, readers[i].get(aMin, aMax, tMin, tMax));
            }
            long endtime = System.currentTimeMillis();
            System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " size: " + (result.size()) + " getMessage: " + (endtime - starttime));
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return result;
    }

    private void init() {
        try {
            System.out.println("init start" + System.currentTimeMillis());
            List<Long> cache = new ArrayList<>();
            TmpWriter tmpWriter = new TmpWriter();
            readers = new Reader[5];
            for (int i = 0; i < 5; i++) {
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
            while (!priorityQueue.isEmpty() && (msgNum < 100000000)) {
                Pair<Message, Writer> pair = priorityQueue.poll();
                tmpWriter.put(pair.fst);
                if (msgNum > 90000000) {
                    cache.add(pair.fst.getA());
                }
                Message newMessage = pair.snd.get();
                if (newMessage != null) {
                    pair.fst = newMessage;
                    priorityQueue.add(pair);
                }
                msgNum++;
            }
            Collections.sort(cache);
            barriers[0] = cache.get(200000);
            barriers[1] = cache.get(400000);
            barriers[2] = cache.get(600000);
            barriers[3] = cache.get(800000);
//            barriers[0] = (0xffffffffffffL + 1) / 4;
//            barriers[1] = barriers[0] * 2;
//            barriers[2] = barriers[0] * 3;
//            barriers[3] = barriers[0] * 4;
            System.out.println("barriers:" + Arrays.toString(barriers));
            Message msg = tmpWriter.get();
            while (msg != null) {
                readers[getBlock(msg.getA())].put(msg);
                msg = tmpWriter.get();
            }
            System.out.println("barriers end" + System.currentTimeMillis());
            cache.clear();
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
//        if (!avg) {
//            synchronized (this) {
//                if (!avg) {
//                    System.out.println("avg:" + System.currentTimeMillis());
//                    avg = true;
//                }
//            }
//        }
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
        if (value > barriers[3]) {
            return 4;
        } else if (value > barriers[2]) {
            return 3;
        } else if (value > barriers[1]) {
            return 2;
        } else if (value > barriers[0]) {
            return 1;
        } else {
            return 0;
        }
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
