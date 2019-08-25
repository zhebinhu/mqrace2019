package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by huzhebin on 2019/08/25.
 */
public class ByteBufferPool {
    public static ByteBuffer[] valueBuffers = new ByteBuffer[Constants.VALUE_BUF_NUM];
    public static AtomicInteger[] valueFlags = new AtomicInteger[Constants.VALUE_BUF_NUM];
    static {
        for(int i=0;i<Constants.VALUE_BUF_NUM;i++){
            valueBuffers[i] = ByteBuffer.allocateDirect(Constants.VALUE_SIZE * (Constants.VALUE_NUM * (i + 1)));
            valueFlags[i] = new AtomicInteger(0);
        }
    }
//    public static ByteBuffer[] dataBuffers = new ByteBuffer[Constants.DATA_BUF_NUM];
//    public static AtomicInteger[] dataFlags = new AtomicInteger[Constants.DATA_BUF_NUM];
//    static {
//        for(int i=0;i<Constants.DATA_BUF_NUM;i++){
//            dataBuffers[i] = ByteBuffer.allocateDirect(Constants.DATA_SIZE * (Constants.DATA_NUM * (i + 1)));
//            dataFlags[i] = new AtomicInteger(0);
//        }
//    }
}
