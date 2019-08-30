package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/17.
 */
public class ValueContext {

    public ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * 80000);

    public byte tag;

    public int nextOffset;

    public int tagIndex;

}
