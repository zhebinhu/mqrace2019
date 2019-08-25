package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class DataContext {
//    public DataContext() {
//        for (int i = 0; i < 80; i++) {
//            bufferList.add(ByteBuffer.allocateDirect(Constants.DATA_SIZE * (Constants.VALUE_NUM * (i + 1))));
//        }
//        buffer = bufferList.get(0);
//    }
//    public List<ByteBuffer> bufferList = new ArrayList<>();
    public ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM);

    public int bufferMaxIndex = 0;

    public int bufferMinIndex = 0;
}