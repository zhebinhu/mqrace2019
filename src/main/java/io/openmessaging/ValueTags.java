package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by huzhebin on 2019/08/09.
 */
public class ValueTags extends Tags {

    private ByteBuffer adds;

    public ValueTags(int cap) {
        super(cap);
        adds = ByteBuffer.allocateDirect(cap);
    }

    public void add(byte add) {
        adds.put(index - 1, add);
    }

    public byte getAdd(int addIndex) {
        return adds.get(addIndex);
    }

}
