package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class Queue {
    /**
     * 队列的编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 直接内存
     */
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    /**
     * 消息总数
     */
    private int messageNum = 0;

    /**
     * 最大时间
     */
    private long maxTime = -1L;

    /**
     * 一级索引
     */
    private List<Index> indexList = new ArrayList<>();

    private volatile boolean inited = false;

    private DataReader dataReader;

    private ValueReader valueReader;

    private Index index = new Index(0, 0);

    //private int arraysLen = 180000000;

    /**
     * 时间标签
     */
    private byte[] times = new byte[Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM];

    public Queue(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".msg", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
        dataReader = new DataReader(num);
        valueReader = new ValueReader(num);

    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.MESSAGE_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }

        if (maxTime == -1 || message.getT() > maxTime + 255) {
            maxTime = message.getT();
            indexList.add(new Index(maxTime, messageNum));
        }

        buffer.put((byte) (message.getT() - maxTime));
        valueReader.put(message);
        dataReader.put(message);

        messageNum++;
    }

    public void init() {
        int remain = buffer.remaining();
        if (remain > 0) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
        }
    }

    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax, MessagePool messagePool) {
        List<Message> result = new ArrayList<>();

        if (indexList.isEmpty()) {
            return result;
        }

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }

        int offsetA;
        int offsetB;
        int i = 0;

        if (tMin <= indexList.get(0).getTime()) {
            offsetA = 0;
        } else {
            i = Collections.binarySearch(indexList, index);
            if (i < 0) {
                i = Math.max(0, -(i + 2));
            }
            offsetA = indexList.get(i).getOffset();
//            byte t = (byte) (tMin - indexList.get(i).getTime());
//            while (t - times[offsetA] > 0) {
//                offsetA++;
//            }
        }
        try {
            buffer.clear();
            fileChannel.read(buffer, offsetA);
            buffer.flip();
            buffer.get(times);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (i == indexList.size() - 1) {
            offsetB = messageNum - offsetA;
        } else {
            offsetB = indexList.get(i).getOffset() - offsetA;
        }
        int baseOffset = offsetA;
        offsetA = 0;
        while (true) {
            if (offsetA == messageNum - offsetA) {
                break;
            }
            if (offsetA == offsetB) {
                i++;
                if (i == indexList.size() - 1) {
                    offsetB = messageNum - offsetA;
                } else {
                    offsetB = indexList.get(i).getOffset() - offsetA;
                }
            }
            long time = indexList.get(i).getOffset() + (times[offsetA] % 255);
            if (time > tMax) {
                break;
            }
            long value = valueReader.getValue(baseOffset + offsetA);
            if (value < aMin || value > aMax) {
                offsetA++;
                continue;
            }
            Message message = messagePool.get();
            dataReader.getData(baseOffset + offsetA, message);
            message.setT(time);
            message.setA(value);
            result.add(message);
        }
        return result;

    }

    public synchronized Avg getAvg(long aMin, long aMax, long tMin, long tMax) {
        Avg result = new Avg();

        if (indexList.isEmpty()) {
            result.setTotal(0);
            result.setCount(0);
            return result;
        }

        //        if (!inited) {
        //            synchronized (this) {
        //                if (!inited) {
        //                    init();
        //                    inited = true;
        //                }
        //            }
        //        }

        long offsetA;
        long offsetB;

        if (tMin / Constants.INDEX_RATE > maxTime) {
            return result;
        } else {
            index.setTime(tMin / Constants.INDEX_RATE);
            int i = Collections.binarySearch(indexList, index);
            if (i >= 0) {
                offsetA = indexList.get(i).getOffset();
            } else {
                offsetA = indexList.get(Math.max(0, -(i + 2))).getOffset();
                while (true) {
                    buffer.clear();
                    try {
                        fileChannel.read(buffer, offsetA * Constants.MESSAGE_SIZE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    buffer.flip();
                    int right = Math.min(Constants.MESSAGE_NUM - 1, buffer.limit());
                    int left = 0;
                    buffer.position(right * Constants.MESSAGE_SIZE);
                    if (buffer.getLong() < tMin) {
                        offsetA = offsetA + Constants.MESSAGE_NUM;
                        continue;
                    } else {
                        while (right - left > 1) {
                            int mid = (right + left) / 2;
                            buffer.position(mid * Constants.MESSAGE_SIZE);
                            if (buffer.getLong() >= tMin) {
                                right = mid;
                            } else {
                                left = mid;
                            }
                        }
                        offsetA = offsetA + right;
                        break;
                    }
                }
            }
        }
        if (tMax / Constants.INDEX_RATE >= maxTime) {
            offsetB = messageNum;
        } else {
            index.setTime(tMax / Constants.INDEX_RATE + 1);
            int i = Collections.binarySearch(indexList, index);
            if (i >= 0) {
                offsetB = indexList.get(i).getOffset();
            } else {
                offsetB = indexList.get(Math.min(indexList.size() - 1, -(i + 1))).getOffset();
                while (true) {
                    buffer.clear();
                    try {
                        fileChannel.read(buffer, (offsetB - Constants.MESSAGE_NUM) * Constants.MESSAGE_SIZE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    buffer.flip();
                    int right = Math.min(Constants.MESSAGE_NUM - 1, buffer.limit());
                    int left = 0;
                    buffer.position(left * Constants.MESSAGE_SIZE);
                    if (buffer.getLong() > tMax) {
                        offsetB = offsetB - Constants.MESSAGE_NUM;
                        continue;
                    } else {
                        while (right - left > 1) {
                            int mid = (right + left) / 2;
                            buffer.position(mid * Constants.MESSAGE_SIZE);
                            if (buffer.getLong() >= tMax) {
                                right = mid;
                            } else {
                                left = mid;
                            }
                        }
                        offsetB = offsetB - Constants.MESSAGE_NUM + right;
                        break;
                    }
                }
            }
        }

        while (offsetA < offsetB) {
            try {
                //                buffer.clear();
                //                fileChannel.read(buffer, offsetA * Constants.MESSAGE_SIZE);
                //                buffer.flip();
                //                long offset = Math.min(offsetB - offsetA, Constants.MESSAGE_NUM);
                //                for (int i = 0; i < offset; i++) {
                //                    long time = buffer.getLong();
                //                    if (time < tMin || time > tMax) {
                //                        buffer.position(buffer.position() + Constants.MESSAGE_SIZE - 8);
                //                        continue;
                //                    }
                long value = valueReader.getValue(offsetA);
                if (value < aMin || value > aMax) {
                    offsetA++;
                    continue;
                }
                result.setCount(result.getCount() + 1);
                result.setTotal(result.getTotal() + value);
                //}
                offsetA++;

            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }
        return result;
    }

}
