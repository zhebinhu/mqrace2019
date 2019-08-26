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
        for (int i = 0; i < 118; i++) {
            bufferList.add(ByteBuffer.allocateDirect(4096 * (i + 1)));
        }
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    public ByteBuffer buffer = ByteBuffer.allocate(Constants.VALUE_SIZE * Constants.VALUE_NUM);

    public int bufferMaxIndex = 0;

    public int bufferMinIndex = 0;
}
