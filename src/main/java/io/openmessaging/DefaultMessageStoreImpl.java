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

    @Override
    public void put(Message message) {
        if (!queues.containsKey(Thread.currentThread())) {
            synchronized (this) {
                if (!queues.containsKey(Thread.currentThread())) {
                    queues.put(Thread.currentThread(), new Queue(num.getAndIncrement()));
                }
            }
        }
        queues.get(Thread.currentThread()).put(message);
    }

    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (forkJoinPool == null) {
            synchronized (this) {
                if (forkJoinPool == null) {
                    forkJoinPool = new ForkJoinPool();
                }
            }
        }
//        try {
//            semaphore.acquire();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        System.out.println("1" + aMin + " " + aMax + " " + tMin + " " + tMax + " " + System.currentTimeMillis());
        List<Message> result = forkJoinPool.invoke(new MergeTask(new ArrayList<>(queues.values()), 0, queues.size() - 1, aMin, aMax, tMin, tMax));
//        semaphore.release();
        return result;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (avgJoinPool == null) {
            synchronized (this) {
                if (avgJoinPool == null) {
                    avgJoinPool = new ForkJoinPool(queues.size() * 2);
                }
            }
        }
        try {
            return (long) avgJoinPool.submit(() -> queues.values().stream().parallel().map(queue -> queue.getMessage(aMin, aMax, tMin, tMax)).flatMap(Collection::stream).mapToLong(Message::getA).average().getAsDouble()).get().doubleValue();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("2" + aMin + " " + aMax + " " + tMin + " " + tMax + " " + System.currentTimeMillis());
        return 0L;
        //return (long)queues.values().stream().parallel().map(queue -> queue.getMessage(aMin, aMax, tMin, tMax)).flatMap(Collection::stream).mapToLong(Message::getA).average().getAsDouble();
    }

    public class MergeTask extends RecursiveTask<List<Message>> {
        private int start;

        private int end;

        private List<Queue> queues;

        private long aMin;

        private long aMax;

        private long tMin;

        private long tMax;

        MergeTask(List<Queue> queues, int start, int end, long aMin, long aMax, long tMin, long tMax) {
            this.queues = queues;
            this.start = start;
            this.end = end;
            this.aMin = aMin;
            this.aMax = aMax;
            this.tMin = tMin;
            this.tMax = tMax;
        }

        protected List<Message> compute() {
            List<Message> result;
            if (start == end) {
                return queues.get(start).getMessage(aMin, aMax, tMin, tMax);
            } else {
                int mid = (start + end) / 2;
                MergeTask leftTask = new MergeTask(queues, start, mid, aMin, aMax, tMin, tMax);
                MergeTask rightTask = new MergeTask(queues, mid + 1, end, aMin, aMax, tMin, tMax);

                leftTask.fork();
                rightTask.fork();

                List<Message> leftResult = leftTask.join();
                List<Message> rightResult = rightTask.join();

                result = merge(leftResult, rightResult);
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
