package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/01.
 */
public class ValuePage {
    //byte[] bytes = new byte[Constants.VALUE_PAGE_SIZE];
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Constants.VALUE_PAGE_SIZE);
}
