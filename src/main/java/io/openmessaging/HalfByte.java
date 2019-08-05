package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/05.
 */
public class HalfByte {
    byte aByte;

    public HalfByte(byte aByte) {
        this.aByte = aByte;
    }

    public void setRight(byte right) {
        aByte &= 0xf0;
        aByte |= (right & 0x0f);
    }

    public void setLeft(byte left) {
        aByte &= 0x0f;
        aByte |= ((left & 0x0f) << 4);
    }

    public byte getRight() {
        return (byte) (aByte & 0x0f);
    }

    public byte getLeft() {
        return (byte) ((aByte & 0xf0) >>> 4);
    }

    public byte getByte() {
        return aByte;
    }

    public void setByte(byte aByte) {
        this.aByte = aByte;
    }
}
