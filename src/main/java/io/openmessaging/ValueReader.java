package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class ValueReader {
    /**
     * 编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 双缓冲异步
     */
    private ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    private ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    private ByteBuffer buffer;

    /**
     * 消息总数
     */
    private long messageNum = 0L;

    /**
     * 缓存中最大消息
     */
    private long bufferMaxIndex = -1L;

    /**
     * 缓存中最小消息
     */
    private long bufferMinIndex = -1L;

    private volatile boolean inited = false;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * 异步put结果
     */
    private Future putFuture;

    public ValueReader(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".value", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
        buffer = buffer1;
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.DATA_SIZE) {
            if (putFuture != null) {
                try {
                    putFuture.get();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                }
            }

            ByteBuffer tmp = buffer;
            putFuture = executorService.submit(() -> {
                try {
                    tmp.flip();
                    fileChannel.write(tmp);
                    tmp.clear();
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
            });
            if (buffer == buffer1) {
                buffer = buffer2;
            } else {
                buffer = buffer1;
            }
        }
        buffer.putLong(message.getA());
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

    public synchronized long getValue(long index) {

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }

        if (index >= bufferMinIndex && index < bufferMaxIndex) {
            buffer.position((int) (index - bufferMinIndex) * Constants.VALUE_SIZE);
        } else {
            buffer.clear();
            try {
                fileChannel.read(buffer, index * Constants.VALUE_SIZE);
                bufferMinIndex = index;
                bufferMaxIndex = Math.min(index + Constants.VALUE_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.flip();
        }

        return buffer.getLong();
    }

}