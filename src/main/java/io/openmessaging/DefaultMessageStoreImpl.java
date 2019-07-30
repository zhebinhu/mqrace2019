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

    /**
     * 一个消费线程对应一个消息池
     */
    //private ConcurrentMap<Thread, MessagePool> messagePools = new ConcurrentHashMap<>();

    private AtomicInteger num = new AtomicInteger(0);

    private volatile boolean put = false;

    private volatile boolean get = false;

    private volatile boolean avg = false;

    private ForkJoinPool forkJoinPool = new ForkJoinPool();

    private ThreadLocal<MessagePool> messagePoolThreadLocal = new ThreadLocal<>();

    private ThreadLocal<MiniMsgPool> miniMsgPoolThreadLocal = new ThreadLocal<>();

    //private Map<Thread,Object> threadSet = new ConcurrentHashMap<>();

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
        List<Message> result = new ArrayList<>();
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
            result = forkJoinPool.submit(new MergeTask(new ArrayList<>(queues.values()), 0, queues.size() - 1, aMin, aMax, tMin, tMax, messagePoolThreadLocal.get())).get();
//            List<List<Message>> messageLists = new ArrayList<>();
//            for (Queue queue : queues.values()) {
//                messageLists.add(queue.getMessage(aMin, aMax, tMin, tMax, messagePoolThreadLocal.get()));
//            }
//            for (List<Message> messages : messageLists) {
//                result = merge(result, messages);
//            }
            //long endtime = System.currentTimeMillis();
            //System.out.println(aMin + " " + aMax + " " + tMin + " " + tMax + " size: " + (result.size()) + " getMessage: " + (endtime - starttime));
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        try {
            if (!avg) {
                synchronized (this) {
                    if (!avg) {
                        System.out.println("avg:" + System.currentTimeMillis());
                        avg = true;
                        //messagePools.clear();
                    }
                }
            }
//            if (!messagePools.containsKey(Thread.currentThread())) {
//                synchronized (this) {
//                    if (!messagePools.containsKey(Thread.currentThread())) {
//                        messagePools.put(Thread.currentThread(), new MessagePool());
//                    }
//                }
//            }
            if (miniMsgPoolThreadLocal.get() == null) {
                miniMsgPoolThreadLocal.set(new MiniMsgPool());
            }
            MiniMsgPool miniMsgPool = miniMsgPoolThreadLocal.get();
            OptionalDouble result = queues.values().stream().parallel().map(queue -> queue.getMiniMsg(aMin, aMax, tMin, tMax, miniMsgPool)).flatMap(Collection::stream).mapToLong(MiniMsg::getA).average();
            if(!result.isPresent()){
                return 0L;
            }
            else {
                return (long)result.getAsDouble();
            }
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
