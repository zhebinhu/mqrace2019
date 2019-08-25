package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huzhebin on 2019/08/25.
 */
public class ByteBufferPool {
    public static ByteBuffer[] byteBuffers = new ByteBuffer[Constants.VALUE_BUF_NUM];

    public static AtomicInteger[] flags = new AtomicInteger[Constants.VALUE_BUF_NUM];

    static {
        for (int i = 0; i < Constants.VALUE_BUF_NUM; i++) {
            byteBuffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * (Constants.VALUE_NUM * (i + 1)));
            flags[i] = new AtomicInteger(0);
        }
    }
}
