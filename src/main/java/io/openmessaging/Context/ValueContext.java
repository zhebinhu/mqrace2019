package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huzhebin on 2019/08/17.
 */
public class ValueContext {
    public ValueContext() {
        for (int i = 0; i < (8 * 80000) / Constants.PAGE_SIZE; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.PAGE_SIZE * (i + 1)));
        }
        buffer = bufferList.get(7);
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    //public ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_BUF_NUM * Constants.VALUE_NUM);

    public ByteBuffer buffer;

    public int msgLen = 0;
}
