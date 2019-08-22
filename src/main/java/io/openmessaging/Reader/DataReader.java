package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Message;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class DataReader {
    /**
     * 编号
     */
    private int num;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 堆外内存
     */
    private ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    private ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    private ByteBuffer buffer;

    private Future future;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;

    public DataReader() {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.data", "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        buffer = buffer1;
    }

    public void put(Message message) {
        ByteBuffer tmpBuffer = buffer;
        if (!buffer.hasRemaining()) {
            buffer.flip();
            try {
                if (future == null) {
                    future = executorService.submit(() -> fileChannel.write(tmpBuffer));
                } else {
                    future.get();
                    future = executorService.submit(() -> fileChannel.write(tmpBuffer));
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            if (buffer == buffer1) {
                buffer = buffer2;
            } else {
                buffer = buffer1;
            }
            buffer.clear();
        }
        buffer.put(message.getBody());
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

    public void getData(int index, Message message, DataContext dataContext) {

        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        if (index >= dataContext.bufferMinIndex && index < dataContext.bufferMaxIndex) {
            dataContext.buffer.position((index - dataContext.bufferMinIndex) * Constants.DATA_SIZE);
        } else {
            dataContext.buffer.clear();
            try {
                fileChannel.read(dataContext.buffer, ((long) index) * Constants.DATA_SIZE);
                dataContext.bufferMinIndex = index;
                dataContext.bufferMaxIndex = Math.min(index + Constants.DATA_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            dataContext.buffer.flip();
        }

        dataContext.buffer.get(message.getBody());
    }

}