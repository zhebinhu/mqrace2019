package io.openmessaging;

import io.openmessaging.Reader.Reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
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
            //            long starttime = System.currentTimeMillis();
            for (int i = getBlock(aMin); i <= getBlock(aMax); i++) {
                //result = reader.get(aMin, aMax, tMin, tMax);
                result = merge(result, readers[i].get(aMin, aMax, tMin, tMax));
            }
            //            long endtime = System.currentTimeMillis();
            //            System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " size: " + (result.size()) + " getMessage: " + (endtime - starttime));
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return result;
    }

    private void init() {

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
        while (!priorityQueue.isEmpty()) {
            Pair<Message, Writer> pair = priorityQueue.poll();
            int block =getBlock(pair.fst.getA());
            //System.out.println(block);
            readers[block].put(pair.fst);
            Message newMessage = pair.snd.get();
            if (newMessage != null) {
                pair.fst = newMessage;
                priorityQueue.add(pair);
            }
        }
        writers.clear();

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
        long a = (0xffffffffffffL + 1) / 4;
        return value > 0xffffffffffffL ? 4 : value < 0 ? 0 : (int) (value / a);
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
