package io.openmessaging.Reader;

import io.openmessaging.Constants;
import io.openmessaging.Context.DataContext;
import io.openmessaging.Message;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
    private ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    /**
     * 消息总数
     */
    private int messageNum = 0;

    private volatile boolean inited = false;

    public DataReader(int num) {
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        fileChannel = memoryMappedFile.getChannel();
    }

    public void put(Message message) {
        int remain = buffer.remaining();
        if (remain < Constants.DATA_SIZE) {
            buffer.flip();
            try {
                fileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace(System.out);
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