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
import java.util.concurrent.*;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class Reader {
    /**
     * 队列的编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 读通道
     */
    private FileChannel readChannel;

    /**
     * 读缓冲区
     */
    private BlockingQueue<ByteBuffer> byteBuffers = new LinkedBlockingQueue(10);

    /**
     * 直接内存
     */
    private ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    private ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    private ByteBuffer buffer;

    private Future future;

    private ExecutorService flushThread = Executors.newSingleThreadExecutor();


    private List<MappedByteBuffer> mappedByteBuffers = new ArrayList<>();

    private MappedByteBuffer mappedByteBuffer;

    private Long index = 0L;

    private Long curTime = -1L;

    public NavigableMap<Long, Long> indexMap = new TreeMap<Long, Long>();

    private volatile boolean inited = false;

    public Reader(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fileChannel = memoryMappedFile.getChannel();

        for (int i = 0; i < Constants.READ_PARALLEL; i++) {
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);
            byteBuffers.add(readBuffer);
        }
        buffer = buffer1;
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.MESSAGE_SIZE) {
            buffer.flip();
            flush();
        }
        buffer.putLong(message.getT());
        buffer.putLong(message.getA());
        buffer.put(message.getBody());
        if (message.getT() / Constants.INDEX_RATE > curTime) {
            curTime = message.getT() / Constants.INDEX_RATE;
            indexMap.put(curTime, index);
        }
        index++;
    }

    public void flush() {
        if (future != null) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ByteBuffer temp = buffer;
        future = flushThread.submit(() -> {
            try {
                fileChannel.write(temp);
                temp.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        if (buffer == buffer1) {
            buffer = buffer2;
        } else {
            buffer = buffer1;
        }
    }

    public void init() {
        int remain = buffer.remaining();
        if (remain > 0) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
                buffer.clear();
                buffer1 = null;
                buffer2 = null;
                buffer = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        RandomAccessFile memoryReadFile = null;
        try {
            memoryReadFile = new RandomAccessFile(Constants.URL + num + ".data", "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        readChannel = memoryReadFile.getChannel();
        //        long mappedIndex = 0;
//        long fileSize = index * Constants.MESSAGE_SIZE;
//        while (mappedIndex < fileSize) {
//            try {
//                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mappedIndex, Math.min(fileSize - mappedIndex, Constants.MAPPED_SIZE));
//                mappedByteBuffers.add(mappedByteBuffer);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            mappedIndex += Constants.MAPPED_SIZE;
//        }
    }

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        ByteBuffer buffer = null;
        try {
            buffer = byteBuffers.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<Message> result = new ArrayList<>();

        if (indexMap.isEmpty()) {
            return result;
        }

        //        int remain = buffer.remaining();
        //        if (remain > 0) {
        //            buffer.flip();
        //            try {
        //                fileChannel.write(buffer);
        //                buffer.clear();
        //            } catch (IOException e) {
        //                e.printStackTrace();
        //            }
        //        }

        if (!inited) {
            synchronized (this) {
                if(!inited) {
                    init();
                    inited = true;
                }
            }
        }

        Long offsetA;
        Long offsetB;
        if (tMin / Constants.INDEX_RATE > curTime) {
            return result;
        } else {
            offsetA = indexMap.get(indexMap.higherKey(tMin / Constants.INDEX_RATE - 1));
        }
        if (tMax / Constants.INDEX_RATE >= curTime) {
            offsetB = index;
        } else {
            offsetB = indexMap.get(indexMap.higherKey(tMax / Constants.INDEX_RATE));
        }

        //        while (offsetA < offsetB) {
        //            mappedByteBuffer = mappedByteBuffers.get((int) (offsetA * Constants.MESSAGE_SIZE / Constants.MAPPED_SIZE));
        //            int start = (int) ((offsetA * Constants.MESSAGE_SIZE) % Constants.MAPPED_SIZE);
        //            int len = (int) Math.min((offsetB - offsetA) * Constants.MESSAGE_SIZE, Constants.MAPPED_SIZE - start);
        //            mappedByteBuffer.position(start);
        //            while (mappedByteBuffer.position() < start + len) {
        //                long time = mappedByteBuffer.getLong();
        //                if (time < tMin || time > tMax) {
        //                    mappedByteBuffer.position(mappedByteBuffer.position() + Constants.MESSAGE_SIZE - 8);
        //                    continue;
        //                }
        //                long value = mappedByteBuffer.getLong();
        //                if (value < aMin || value > aMax) {
        //                    mappedByteBuffer.position(mappedByteBuffer.position() + Constants.MESSAGE_SIZE - 16);
        //                    continue;
        //                }
        //                byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
        //                mappedByteBuffer.get(body);
        //                Message message = new Message(value, time, body);
        //                result.add(message);
        //            }
        //            offsetA += len / Constants.MESSAGE_SIZE;
        //        }

        //        try {
        //            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offsetA * Constants.MESSAGE_SIZE, (offsetB - offsetA) * Constants.MESSAGE_SIZE);
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        }
        //        while (offsetA < offsetB) {
        //            long time = mappedByteBuffer.getLong();
        //            if (time < tMin || time > tMax) {
        //                mappedByteBuffer.position(mappedByteBuffer.position() + Constants.MESSAGE_SIZE - 8);
        //                offsetA++;
        //                continue;
        //            }
        //            long value = mappedByteBuffer.getLong();
        //            if (value < aMin || value > aMax) {
        //                mappedByteBuffer.position(mappedByteBuffer.position() + Constants.MESSAGE_SIZE - 16);
        //                offsetA++;
        //                continue;
        //            }
        //            byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
        //            mappedByteBuffer.get(body);
        //            Message message = new Message(value, time, body);
        //            result.add(message);
        //            offsetA++;
        //        }

        while (offsetA < offsetB) {
            try {
                readChannel.read(buffer, offsetA * Constants.MESSAGE_SIZE);
                buffer.flip();
                long offset = Math.min(offsetB - offsetA, Constants.MESSAGE_NUM);
                for (int i = 0; i < offset; i++) {
                    long time = buffer.getLong();
                    if (time < tMin || time > tMax) {
                        buffer.position(buffer.position() + Constants.MESSAGE_SIZE - 8);
                        continue;
                    }
                    long value = buffer.getLong();
                    if (value < aMin || value > aMax) {
                        buffer.position(buffer.position() + Constants.MESSAGE_SIZE - 16);
                        continue;
                    }
                    byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
                    buffer.get(body);
                    Message message = new Message(value, time, body);
                    result.add(message);

                }
                buffer.clear();
                offsetA += Constants.MESSAGE_NUM;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        buffer.clear();
        byteBuffers.add(buffer);
        return result;

    }
}
