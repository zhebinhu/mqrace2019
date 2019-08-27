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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class DataReader {
    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    private final int bufNum = 8;

    /**
     * 堆外内存
     */
    private ByteBuffer[] buffers = new ByteBuffer[bufNum];

    private Future[] futures = new Future[bufNum];

    private int index = 0;

    private ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setPriority(10);
        return thread;
    });

    /**
     * 消息总数
     */
    private int messageNum = 0;

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
            for (Future future : futures) {
                if (!future.isDone()) {
                    future.get();
                }
            }
            if (buffers[index].hasRemaining()) {
                buffers[index].flip();
                fileChannel.write(buffers[index]);
                buffers[index].clear();
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public void getData(int index, Message message, DataContext dataContext) {
        //            if (index >= dataContext.bufferMinIndex && index < dataContext.bufferMaxIndex) {
        //                dataContext.buffer.position((index - dataContext.bufferMinIndex) * Constants.DATA_SIZE);
        //            } else {
        //                dataContext.buffer.clear();
        //                try {
        //                    fileChannel.read(dataContext.buffer, ((long) index) * Constants.DATA_SIZE);
        //                    dataContext.bufferMinIndex = index;
        //                    dataContext.bufferMaxIndex = Math.min(index + Constants.DATA_NUM, messageNum);
        //                } catch (IOException e) {
        //                    e.printStackTrace(System.out);
        //                }
        //                dataContext.buffer.flip();
        //            }
        //
        //            dataContext.buffer.get(message.getBody());
        int i = (index - dataContext.offsetA) / Constants.DATA_NUM;
        if(!dataContext.futures[i].isDone()){
            try {
                dataContext.futures[i].get();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }
        dataContext.bufferList.get(i).get(message.getBody());
    }

    public void updateContext(int offsetA, int offsetB, DataContext dataContext) {
        int num = (offsetB - offsetA) / Constants.DATA_NUM + 1;
        //        dataContext.buffer = dataContext.bufferList.get(i);
        //        dataContext.buffer.clear();
        //        try {
        //            fileChannel.read(dataContext.buffer, ((long) offsetA) * Constants.DATA_SIZE);
        //        } catch (IOException e) {
        //            e.printStackTrace(System.out);
        //        }
        //        dataContext.buffer.flip();
        for (int i = 0; i < num; i++) {
            final int f = i;
            dataContext.futures[i] = dataContext.executorService.submit(() -> {
                dataContext.bufferList.get(f).clear();
                try {
                    fileChannel.read(dataContext.bufferList.get(f), ((long)offsetA + f * Constants.DATA_NUM) * Constants.DATA_SIZE);
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
                dataContext.bufferList.get(f).flip();
            });
        }
        dataContext.offsetA = offsetA;
    }

}