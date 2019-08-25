package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class DataContext {
    public DataContext() {
        for (int i = 0; i < Constants.DATA_BUF_NUM; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.DATA_SIZE * (Constants.DATA_NUM * (i + 1))));
        }
        buffer = bufferList.get(0);
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    public ByteBuffer buffer;

}