package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

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

    private ThreadLocal<ByteBuffer> readBuffer = new ThreadLocal<>();

    /**
     * 消息总数
     */
    private int messageNum = 0;

    /**
     * 缓存中最大消息
     */
    private ThreadLocal<Integer> bufferMaxIndex = new ThreadLocal<>();

    /**
     * 缓存中最小消息
     */
    private ThreadLocal<Integer> bufferMinIndex = new ThreadLocal<>();

    private ThreadLocal<Integer> tagMinIndex = new ThreadLocal<>();

    private ThreadLocal<Integer> tagMaxIndex = new ThreadLocal<>();

    private volatile boolean inited = false;

    private ThreadLocal<byte[]> zipByte = new ThreadLocal<>();

    private ThreadLocal<byte[]> dataTag = new ThreadLocal<>();

    private List<DataTag> dataTags = new ArrayList<>();

    public DataReader(int num) {
        this.num = num;
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
        if (dataTags.isEmpty() || !canZip(dataTag.get(), message.getBody())) {
            dataTag.set(new byte[34]);
            System.arraycopy(message.getBody(), 0, dataTag.get(), 0, 34);
            dataTags.add(new DataTag(message.getBody(), messageNum));
        }
        if (zipByte.get() == null) {
            zipByte.set(new byte[Constants.DATA_SIZE]);
        }
        zip(dataTag.get(), message.getBody(), zipByte.get());
        buffer.put(zipByte.get());
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
        //System.out.println("Thread:" + num + " timeTagList size:" + dataTags.size());

    }

    public void getData(int index, Message message) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        if (tagMinIndex.get() == null) {
            tagMinIndex.set(-1);
        }
        if (tagMaxIndex.get() == null) {
            tagMaxIndex.set(-1);
        }
        if (bufferMinIndex.get() == null) {
            bufferMinIndex.set(-1);
        }
        if (bufferMaxIndex.get() == null) {
            bufferMaxIndex.set(-1);
        }
        if (readBuffer.get() == null) {
            readBuffer.set(ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM));
        }
        if(zipByte.get()==null){
            zipByte.set(new byte[Constants.DATA_SIZE]);
        }

        if (index == tagMinIndex.get()) {
            System.arraycopy(dataTag.get(), 0, message.getBody(), 0, 34);
            return;
        }

        if (index >= bufferMinIndex.get() && index < bufferMaxIndex.get()) {
            readBuffer.get().position((index - bufferMinIndex.get()) * Constants.DATA_SIZE);
        } else {
            readBuffer.get().clear();
            try {
                fileChannel.read(readBuffer.get(), index * Constants.DATA_SIZE);
                bufferMinIndex.set(index);
                bufferMaxIndex.set(Math.min(index + Constants.DATA_NUM, messageNum));
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            readBuffer.get().flip();
        }
        readBuffer.get().get(zipByte.get());


        if (index > tagMinIndex.get() && index < tagMaxIndex.get()) {
            unZip(dataTag.get(), zipByte.get(), message.getBody());
            return;
        }

        int thisIndex = Collections.binarySearch(dataTags, new DataTag(null, index));
        if (thisIndex >= 0) {
            dataTag.set(dataTags.get(thisIndex).getData());
            System.arraycopy(dataTag.get(), 0, message.getBody(), 0, 34);
            tagMinIndex.set(dataTags.get(thisIndex).getOffset());
            if (thisIndex == dataTags.size() - 1) {
                tagMaxIndex.set(messageNum);
            } else {
                tagMaxIndex.set(dataTags.get(thisIndex + 1).getOffset());
            }
            return;
        }
        if (thisIndex < 0) {
            thisIndex = Math.max(0, -(thisIndex + 2));
            dataTag.set(dataTags.get(thisIndex).getData());
            unZip(dataTag.get(), zipByte.get(), message.getBody());
            tagMinIndex.set(dataTags.get(thisIndex).getOffset());
            if (thisIndex == dataTags.size() - 1) {
                tagMaxIndex.set(messageNum);
            } else {
                tagMaxIndex.set(dataTags.get(thisIndex + 1).getOffset());
            }
        }

    }

    private boolean canZip(byte[] dataTag, byte[] data) {
        int diff = 0;
        int i = 2;
        while (i < 34) {
            if (dataTag[i] != data[i]) {
                diff++;
                i = 4 - ((i - 2) % 4) + i;
            } else {
                i++;
            }
        }
        return diff < 3;
    }

    private void zip(byte[] dataTag, byte[] data, byte[] zipByte) {
        zipByte[0] = data[0];
        zipByte[1] = data[1];
        int i = 2;
        byte bitmap = 0;
        byte b = 1;
        byte p = 3;
        while (i < 34) {
            if (dataTag[i] != data[i]) {
                int j = (i - 2) / 4;
                bitmap |= (b << j);
                zipByte[p++] = data[2 + 4 * j];
                zipByte[p++] = data[3 + 4 * j];
                zipByte[p++] = data[4 + 4 * j];
                zipByte[p++] = data[5 + 4 * j];
                i = 4 - ((i - 2) % 4) + i;
            } else {
                i++;
            }
        }
        zipByte[2] = bitmap;
    }

    private void unZip(byte[] dataTag, byte[] zipData, byte[] data) {
        data[0] = zipData[0];
        data[1] = zipData[1];
        byte i = 3;
        byte j = 2;
        byte b = 1;
        for (byte k = 0; k < 8; k++) {
            if ((zipData[2] & (b << k)) != 0) {
                data[j++] = zipData[i++];
                data[j++] = zipData[i++];
                data[j++] = zipData[i++];
                data[j++] = zipData[i++];
            } else {
                data[j++] = dataTag[k * 4 + 2];
                data[j++] = dataTag[k * 4 + 3];
                data[j++] = dataTag[k * 4 + 4];
                data[j++] = dataTag[k * 4 + 5];
            }
        }
    }

}