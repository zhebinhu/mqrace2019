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
    public FileChannel fileChannel;

    {
        try {
            fileChannel = new RandomAccessFile(Constants.URL + "100.value", "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
    }

    public ValueContext() {
        for (int i = 0; i < 80; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.VALUE_SIZE * (Constants.VALUE_NUM * (i + 1))));
        }
        buffer = bufferList.get(0);
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    public ByteBuffer buffer;
}
