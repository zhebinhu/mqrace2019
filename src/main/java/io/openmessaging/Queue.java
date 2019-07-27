package io.openmessaging;

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
    private ByteBuffer buffer1 = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    private ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.MESSAGE_SIZE * Constants.MESSAGE_NUM);

    private ByteBuffer buffer;

    private Future future;

    private ExecutorService flushThread = Executors.newSingleThreadExecutor();

    /**
     * 消息总数
     */
    private Long index = 0L;

    private boolean inited = false;

    private Long copied = 0L;

    private Long readed = 0L;

    public Queue(int num) {
        this.num = num;
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(Constants.URL + num + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fileChannel = memoryMappedFile.getChannel();
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
        index++;
    }

    public void flush() {
        if (future != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
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

    public Message copy() {
        if (!inited) {
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
            inited = true;
        }
        if (readed >= index) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
        if (readed.equals(copied)) {
            try {
                buffer.clear();
                fileChannel.read(buffer, copied * Constants.MESSAGE_SIZE);
                buffer.flip();
                copied += Constants.MESSAGE_NUM;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long time = buffer.getLong();
        long value = buffer.getLong();
        byte[] body = new byte[Constants.MESSAGE_SIZE - 16];
        buffer.get(body);
        readed++;
        return new Message(value, time, body);
    }
}
