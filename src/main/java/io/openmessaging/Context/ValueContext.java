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
        for (int i = 0; i < Constants.VALUE_BUF_NUM; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.VALUE_SIZE * (Constants.VALUE_NUM * (i + 1))));
        }
        buffer = bufferList.get(0);
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    //public ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_BUF_NUM * Constants.VALUE_NUM);

    public ByteBuffer buffer;

    public int bufferMaxIndex = 0;

    public int bufferMinIndex = 0;
}
