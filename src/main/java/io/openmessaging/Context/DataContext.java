package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class DataContext {
    public FileChannel fileChannel;

    {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.data", "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
    }

    public DataContext() {
        for (int i = 0; i < 40; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.DATA_SIZE * (Constants.DATA_NUM * (i + 1))));
        }
        buffer = bufferList.get(0);
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();
    public ByteBuffer buffer;

    public int bufferMaxIndex = 0;

    public int bufferMinIndex = 0;
}
