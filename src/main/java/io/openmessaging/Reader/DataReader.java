package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Context.ValueContext;
import io.openmessaging.Message;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class DataReader {
    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    private final static int bufNum = 8;

    /**
     * 堆外内存
     */
    private ByteBuffer[] buffers = new ByteBuffer[bufNum];

    private Future[] futures = new Future[bufNum];

    private int index = 0;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
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
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.DATA_CAP);
        }
    }

    public void put(Message message) {
        if (!buffers[index].hasRemaining()) {
            ByteBuffer tmpBuffer = buffers[index];
            int newIndex = (index + 1) % bufNum;
            tmpBuffer.flip();
            try {
                if (futures[index] == null) {
                    futures[index] = executorService.submit(() -> fileChannel.write(tmpBuffer));
                } else {
                    if (!futures[newIndex].isDone()) {
                        System.out.println("data block");
                        futures[newIndex].get();
                    }
                    futures[index] = executorService.submit(() -> fileChannel.write(tmpBuffer));
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            index = newIndex;
            buffers[index].clear();
        }
        buffers[index].put(message.getBody());
        messageNum++;
    }

    public void init() {
        try {
            int remain = buffers[index].remaining();
            if (remain > 0) {
                buffers[index].flip();
                fileChannel.write(buffers[index]);
                buffers[index].clear();
            }
            for (Future future : futures) {
                if (!future.isDone()) {
                    future.get();
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    private void updateContext(int offsetA, int offsetB, DataContext dataContext) {
        int i = (offsetB - offsetA) / Constants.DATA_NUM;
        dataContext.buffer = dataContext.bufferList.get(i);
    }

    public void pre(int offsetA, int offsetB, DataContext dataContext) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        updateContext(offsetA, offsetB, dataContext);
        dataContext.fileChannel = fileChannel;
        dataContext.buffer.clear();
        try {
            dataContext.fileChannel.read(dataContext.buffer, ((long) offsetA) * Constants.DATA_SIZE);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }
        dataContext.buffer.flip();
        System.out.println("data pre:" + dataContext.buffer.position() + " " + dataContext.buffer.limit() + " " + dataContext.buffer.capacity() + " offsetA:" + offsetA + " offsetB:" + offsetB);
    }

}