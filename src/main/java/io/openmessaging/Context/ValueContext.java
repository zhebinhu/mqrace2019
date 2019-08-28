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
        for (int i = 0; i < (Constants.VALUE_SIZE * 80000) / Constants.PAGE_SIZE; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.PAGE_SIZE * (i + 1)));
        }
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    public ByteBuffer buffer;

    public byte tag;

    public int nextOffset;

    public int tagIndex;

}
