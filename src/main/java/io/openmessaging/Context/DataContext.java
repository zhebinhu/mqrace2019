package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class DataContext {

    public ByteBuffer buffer;

    public ByteBuffer buffer2 = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM * Constants.DATA_BUF_NUM);

}