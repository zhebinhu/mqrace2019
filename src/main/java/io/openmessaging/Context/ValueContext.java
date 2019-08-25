package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/17.
 */
public class ValueContext {

    public ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * Constants.VALUE_BUF_NUM * Constants.VALUE_NUM);

    public ByteBuffer buffer = buffer2;

}
