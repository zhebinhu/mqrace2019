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

    /**
     * 消息总数
     */
    private int messageNum = 0;

    /**
     * 缓存中最大消息
     */
    private int bufferMaxIndex = -1;

    /**
     * 缓存中最小消息
     */
    private int bufferMinIndex = -1;

    private int tagMinIndex = -1;

    private int tagMaxIndex = -1;

    private volatile boolean inited = false;

    private byte[] zipByte = new byte[Constants.DATA_SIZE];

    private byte[] dataTag = new byte[34];

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
                e.printStackTrace();
            }
            buffer.clear();
        }
        if (dataTags.isEmpty() || !canZip(dataTag, message.getBody())) {
            dataTag = message.getBody();
            dataTags.add(new DataTag(message.getBody(), messageNum));
        }
        zip(dataTag, message.getBody(), zipByte);
        buffer.put(zipByte);
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
        System.out.println("Thread:" + num + " timeTagList size:" + dataTags.size());

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
        if (index == tagMinIndex) {
            System.arraycopy(dataTag,0,message.getBody(),0,Constants.DATA_SIZE);
            return;
        }

        if (index >= bufferMinIndex && index < bufferMaxIndex) {
            buffer.position((index - bufferMinIndex) * Constants.DATA_SIZE);
        } else {
            buffer.clear();
            try {
                fileChannel.read(buffer, index * Constants.DATA_SIZE);
                bufferMinIndex = index;
                bufferMaxIndex = Math.min(index + Constants.DATA_NUM, messageNum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            buffer.flip();
        }

        buffer.get(zipByte);

        if (index > tagMinIndex && index < tagMaxIndex) {
            unZip(dataTag, zipByte, message.getBody());
            return;
        }

        int thisIndex = Collections.binarySearch(dataTags, new DataTag(null, index));
        if (thisIndex >= 0) {
            dataTag = dataTags.get(thisIndex).getData();
            System.arraycopy(dataTag,0,message.getBody(),0,Constants.DATA_SIZE);
            tagMinIndex = dataTags.get(thisIndex).getOffset();
            if (thisIndex == dataTags.size() - 1) {
                tagMaxIndex = messageNum;
            } else {
                tagMaxIndex = dataTags.get(thisIndex + 1).getOffset();
            }
            return;
        }
        if (thisIndex < 0) {
            thisIndex = Math.max(0, -(thisIndex + 2));
            dataTag = dataTags.get(thisIndex).getData();
            unZip(dataTag, zipByte, message.getBody());
            tagMinIndex = dataTags.get(thisIndex).getOffset();
            if (thisIndex == dataTags.size() - 1) {
                tagMaxIndex = messageNum;
            } else {
                tagMaxIndex = dataTags.get(thisIndex + 1).getOffset();
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
        int bitmap = 0;
        int b = 1;
        int p = 3;
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
        zipByte[2] = (byte) bitmap;
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