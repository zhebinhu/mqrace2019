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

    byte[] zipByte = new byte[19];

    byte[] dataTag = new byte[34];

    List<DataTag> dataTags = new ArrayList<>();

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
            message.setBody(dataTag);
            return;
        }

        if (index >= bufferMinIndex && index < bufferMaxIndex) {
            buffer.position((int) (index - bufferMinIndex) * Constants.DATA_SIZE);
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
            message.setBody(dataTags.get(thisIndex).getData());
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
            tagMinIndex = dataTags.get(thisIndex).getOffset();
            if (thisIndex == dataTags.size() - 1) {
                tagMaxIndex = messageNum;
            } else {
                tagMaxIndex = dataTags.get(thisIndex + 1).getOffset();
            }
        }

        dataTag = dataTags.get(thisIndex).getData();
        unZip(dataTag, zipByte, message.getBody());
    }

    private boolean canZip(byte[] dataTag, byte[] data) {
        int diff = 0;
        int i = 2;
        while (i < 34) {
            if (dataTag[i] != data[i]) {
                diff++;
                i = 4 - ((i - 2) % 4) + i;
            }
            i++;
        }
        return diff < 4;
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
                if (i < 6) {
                    bitmap |= b;
                    zipByte[p++] = data[2];
                    zipByte[p++] = data[3];
                    zipByte[p++] = data[4];
                    zipByte[p++] = data[5];
                    i = 6;
                } else if (i < 10) {
                    bitmap |= (b << 1);
                    zipByte[p++] = data[6];
                    zipByte[p++] = data[7];
                    zipByte[p++] = data[8];
                    zipByte[p++] = data[9];
                    i = 10;
                } else if (i < 14) {
                    bitmap |= (b << 2);
                    zipByte[p++] = data[10];
                    zipByte[p++] = data[11];
                    zipByte[p++] = data[12];
                    zipByte[p++] = data[13];
                    i = 14;
                } else if (i < 18) {
                    bitmap |= (b << 3);
                    zipByte[p++] = data[14];
                    zipByte[p++] = data[15];
                    zipByte[p++] = data[16];
                    zipByte[p++] = data[17];
                    i = 18;
                } else if (i < 22) {
                    bitmap |= (b << 4);
                    zipByte[p++] = data[18];
                    zipByte[p++] = data[19];
                    zipByte[p++] = data[20];
                    zipByte[p++] = data[21];
                    i = 22;
                } else if (i < 26) {
                    bitmap |= (b << 5);
                    zipByte[p++] = data[22];
                    zipByte[p++] = data[23];
                    zipByte[p++] = data[24];
                    zipByte[p++] = data[25];
                    i = 26;
                } else if (i < 30) {
                    bitmap |= (b << 6);
                    zipByte[p++] = data[26];
                    zipByte[p++] = data[27];
                    zipByte[p++] = data[28];
                    zipByte[p++] = data[29];
                    i = 30;
                } else if (i < 34) {
                    bitmap |= (b << 7);
                    zipByte[p++] = data[30];
                    zipByte[p++] = data[31];
                    zipByte[p++] = data[32];
                    zipByte[p++] = data[33];
                    i = 34;
                }
            }
            i++;
        }
        zipByte[2] = bitmap;
    }

    private void unZip(byte[] dataTag, byte[] zipData, byte[] data) {
        data[0] = zipData[0];
        data[1] = zipData[1];
        int i = 3;
        int j = 2;
        int b = 1;
        for (int k = 0; k < 8; k++) {
            if ((zipData[2] & b << k) != 0) {
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