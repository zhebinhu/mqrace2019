package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

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

    private List<MappedByteBuffer> mappedByteBuffers = new ArrayList<>();

    private MappedByteBuffer mappedByteBuffer;

    private Long index = 0L;

    private Long curTime = -1L;

    private NavigableMap<Long, Long> indexMap = new TreeMap<Long, Long>();

    private boolean inited = false;

    private AtomicInteger count = new AtomicInteger(0);

    public Queue(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.MESSAGE_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        buffer.putLong(message.getT());
        buffer.putLong(message.getA());
        buffer.put(message.getBody());
        if (message.getT() > curTime) {
            curTime = message.getT();
            indexMap.put(curTime, index);
        }
        index++;
        if (count.intValue() % 100000 == 0) {
            System.out.println("thread-" + num + " count:" + count.getAndIncrement() + " time:" + System.currentTimeMillis() + " indexMapSize:" + indexMap.size());
            System.out.println("message:"+message);
        }
    }

    public void init() {
        int remain = buffer.remaining();
        if (remain > 0) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long mappedIndex = 0;
        long fileSize = index * Constants.MESSAGE_SIZE;
        while (mappedIndex < fileSize) {
            try {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mappedIndex, Math.min(fileSize - mappedIndex, Constants.MAPPED_SIZE));
                mappedByteBuffers.add(mappedByteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            mappedIndex += Constants.MAPPED_SIZE;
        }
    }

    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> result = new ArrayList<>();
        if (indexMap.isEmpty()) {
            return result;
        }

        int remain = buffer.remaining();
        if (remain > 0) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //        if (!inited) {
        //            init();
        //            inited = true;
        //        }

        Long offsetA;
        Long offsetB;
        if (tMin > curTime) {
            return result;
        } else {
            offsetA = indexMap.get(indexMap.higherKey(tMin - 1));
        }
        if (tMax >= curTime) {
            offsetB = index;
        } else {
            offsetB = indexMap.get(indexMap.higherKey(tMax));
        }

        //        while (offsetA < offsetB) {
        //            mappedByteBuffer = mappedByteBuffers.get((int) (offsetA * Constants.MESSAGE_SIZE / Constants.MAPPED_SIZE));
        //            int start = (int) ((offsetA * Constants.MESSAGE_SIZE) % Constants.MAPPED_SIZE);
        //            int len = (int) Math.min((offsetB - offsetA) * Constants.MESSAGE_SIZE, Constants.MAPPED_SIZE - start);
        //            mappedByteBuffer.position(start);
        //            while (mappedByteBuffer.position() < start + len) {
        //                Message message = new Message(0, 0, null);
        //                message.setT(mappedByteBuffer.getLong());
        //                message.setA(mappedByteBuffer.getLong());
        //                byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
        //                mappedByteBuffer.get(body);
        //                message.setBody(body);
        //                if (message.getA() <= aMax && message.getA() >= aMin) {
        //                    result.add(message);
        //                }
        //            }
        //            offsetA += len / Constants.MESSAGE_SIZE;
        //        }

        //                try {
        //                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offsetA * Constants.MESSAGE_SIZE, (offsetB - offsetA) * Constants.MESSAGE_SIZE);
        //                } catch (IOException e) {
        //                    e.printStackTrace();
        //                }
        //                while (offsetA < offsetB) {
        //                    Message message = new Message(0, 0, null);
        //                    message.setT(mappedByteBuffer.getLong());
        //                    message.setA(mappedByteBuffer.getLong());
        //                    byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
        //                    mappedByteBuffer.get(body);
        //                    message.setBody(body);
        //                    if (message.getA() <= aMax && message.getA() >= aMin) {
        //                        result.add(message);
        //                    }
        //                    offsetA++;
        //                }

        while (offsetA < offsetB) {
            try {
                fileChannel.read(buffer, offsetA * Constants.MESSAGE_SIZE);
                buffer.flip();
                long offset = Math.min(offsetB - offsetA, Constants.MESSAGE_NUM);
                for (int i = 0; i < offset; i++) {
                    Message message = new Message(0, 0, null);
                    message.setT(buffer.getLong());
                    message.setA(buffer.getLong());
                    byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
                    buffer.get(body);
                    message.setBody(body);
                    if (message.getA() <= aMax && message.getA() >= aMin) {
                        result.add(message);
                    }
                }
                buffer.clear();
                offsetA += Constants.MESSAGE_NUM;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        buffer.clear();

        return result;

    }
}
