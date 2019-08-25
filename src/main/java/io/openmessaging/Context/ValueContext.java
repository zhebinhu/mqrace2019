package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/17.
 */
public class ValueContext {
    public ValueContext() {
        for (int i = 0; i < Constants.VALUE_BUF_NUM; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.VALUE_SIZE * (Constants.VALUE_NUM * (i + 1))));
        }
        buffer = bufferList.get(7);
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    public FileChannel fileChannel;

    {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.value", "r").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public ByteBuffer buffer;

    public int bufferMaxIndex = 0;

    public int bufferMinIndex = 0;
}
