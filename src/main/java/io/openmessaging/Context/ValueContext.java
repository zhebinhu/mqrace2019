package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/17.
 */
public class ValueContext {
    public ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_NUM);

    public int bufferMaxIndex = 0;

    public int bufferMinIndex = 0;
}
