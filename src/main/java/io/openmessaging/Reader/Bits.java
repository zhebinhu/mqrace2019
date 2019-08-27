package io.openmessaging.Reader;

/**
 * Created by huzhebin on 2019/08/27.
 */
public class Bits {
    /*
     * 该方法提供了两个参数，第一个参数是一个byte型数组b，其中已经存放了字节数据，第二个参数off为数据读取的起点位置，从数组off索引出取出数据，如果为1则
     * 返回true，否则返回false
     */
    static boolean getBoolean(byte[] b, int off) {
        return b[off] != 0;
    }

    /*
     * 该方法提供了两个参数，第一个参数是一个byte型数组b，其中已经存放了字节数据，第二个参数off为数据读取的起点位置，从数组off索引处连续取出两个字节的数据
     * 假设第一个数据为c(b[off]),第二个数据为d(b[off+1])，通过一些小操作，将两个字节的数据组成一个char型数据，并返回，下面描述一下具体的操作，之后方法中的
     * 操作类似，便不在细讲了。
     * 假设我们现在用的是32位的机器，我们读取的第一个byte数据为00000000(高八位)，第二个读取的byte数据为01100001(低八位)。
     * 先来看第一步(b[off+1] & 0xff)，这里讲第二个数据与0xff进行了一次与操作，具体过程如下：
     * byte[off+1] :00000000 00000000 00000000 01100001
     *        0xff :00000000 00000000 00000000 11111111  &
     *        结果 :00000000 00000000 00000000 01100001
     * 第二步(b[off] << 8)，具体过程如下：
     *   byte[off] :00000000 00000000 00000000 00000000
     *                                                   << 8
     *        结果 :00000000 00000000 00000000 00000000
     * 第三步两者相加，具体过程如下：
     * byte[off+1] && 0xff :00000000 00000000 00000000 01100001
     *   byte[off] << 8    :00000000 00000000 00000000 00000000   +
     *                结果 :00000000 00000000 00000000 01100001
     * 第四步转换成char型：因为我们知道，java中char型占用两个字节，所有有效数字为后16位，即00000000 01100001，即十进制的97，即字母a。低位的&0xff操作是为了
     * 将非有效位的值全变为0，避免负数时，自动补1对运算的影响。
     * 最终将得到的char型返回。
     */
    static char getChar(byte[] b, int off) {
        return (char) ((b[off + 1] & 0xFF) +
                (b[off] << 8));
    }

    /*
     * 该方法提供两个参数，第一个参数是一个byte型数组，其中已经存放了字节数据，第二个参数off为数据读取的起点位置。每次从数组中读取两个字节的数据，第一次
     * 读取的数据作为short型的高八位，第二次的读取的数据作为short型的低八位，然后通过(short)(((b[off+1])&0xff)+(b[off]<<8))的操作，将两个字节的数据组合
     * 成一个short型数据
     */
    static short getShort(byte[] b, int off) {
        return (short) ((b[off + 1] & 0xFF) +
                (b[off] << 8));
    }

    /*
     * 该方法提供两个参数，第一个参数是一个byte型数组，其中已经存放了字节数据，第二个参数off为数据读取的起点位置。每次从数组中读取四个字节的数据，第一次
     * 读取的数据作为int型的高24~32位，第二次的读取的数据作为int型的16~24位,以此类推，然后通过一定的的位运算，将四个字节的数据组合成一个int型数据并返回。
     */
    static int getInt(byte[] b, int off) {
        return ((b[off + 3] & 0xFF)      ) +
                ((b[off + 2] & 0xFF) <<  8) +
                ((b[off + 1] & 0xFF) << 16) +
                ((b[off    ]       ) << 24);
    }

    /*
     * 该方法提供两个参数，第一个参数是一个byte型数组，其中已经存放了字节数据，第二个参数off为数据读取的起点位置。先是采用getInt的方法，从数组中得到一个
     * int型数据，然后通过Float.intBitsToFloat方法将读取的数据转换成一个float型数据，然后返回。
     */
    static float getFloat(byte[] b, int off) {
        return Float.intBitsToFloat(getInt(b, off));
    }

    /*
     * 该方法提供两个参数，第一个参数是一个byte型数组，其中已经存放了字节数据，第二个参数off为数据读取的起点位置。每次从数组中读取八个字节的数据，第一次
     * 读取的数据作为long型的高56~64位，第二次的读取的数据作为long型的48~56位,以此类推，然后通过一定的的位运算，将八个字节的数据组合成一个long型数据并返
     * 回。
     */
    static long getLong(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL)      ) +
                ((b[off + 6] & 0xFFL) <<  8) +
                ((b[off + 5] & 0xFFL) << 16) +
                ((b[off + 4] & 0xFFL) << 24) +
                ((b[off + 3] & 0xFFL) << 32) +
                ((b[off + 2] & 0xFFL) << 40) +
                ((b[off + 1] & 0xFFL) << 48) +
                (((long) b[off])      << 56);
    }

    /*
     * 该方法提供两个参数，第一个参数是一个byte型数组，其中已经存放了字节数据，第二个参数off为数据读取的起点位置。先是采用getLong的方法，从数组中得到一个
     * long型数据，然后通过Double.longBitsToDouble方法将读取的数据转换成一个double型数据，然后返回。
     */
    static double getDouble(byte[] b, int off) {
        return Double.longBitsToDouble(getLong(b, off));
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写入的boolean型值。执行过程中，首先
     * 判断boolean型参数val的值，如果为true，则向缓存区中写入1，否则写入0，写入时将0或1转换为byte型数据。
     */
    static void putBoolean(byte[] b, int off, boolean val) {
        b[off] = (byte) (val ? 1 : 0);
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写人的char型数据。因为是第一个涉及
     * 位运算的put方法，所以本次详细描述操作过程，后面类似的put方法便不再详细描述了，下面来讲述具体的执行过程。
     * 假设我们执行将一个char型数据'a'放入，那么过程如下：
     * char型数据'a'：00000000 00000000 00000000 01100001
     * 第一步，b[off + 1] = (byte)(val)：
     * 该操作将缓存区中起点位置的下一个数组元素中置入char型数据的低八位，即 01100001。
     * 第二步，b[off] = (byte)(val >>>8)：
     * 将char型数据进行无符号右移8位，此时char型数据的高八位数据位于低八位中，将数据存入数据缓存区b中。
     * 此时数据缓存区b分别存放了char行数据的高低八位数据。需要时，使用对应的getChar方法便可读取数据。
     */
    static void putChar(byte[] b, int off, char val) {
        b[off + 1] = (byte) (val      );
        b[off    ] = (byte) (val >>> 8);
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写人的short型数据。
     * 通过位运算分别将short型数据的高低八位存放于传入的byte型数组之中。需要时，使用对应的getShort方法便可读取数据。
     */
    static void putShort(byte[] b, int off, short val) {
        b[off + 1] = (byte) (val      );
        b[off    ] = (byte) (val >>> 8);
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写人的int型数据。
     * 通过位运算分别将int型数据的32位数据，每八位一组，存放于传入的byte型数组之中。需要时，使用对应的getInt方法便可读取数据。
     */
    static void putInt(byte[] b, int off, int val) {
        b[off + 3] = (byte) (val       );
        b[off + 2] = (byte) (val >>>  8);
        b[off + 1] = (byte) (val >>> 16);
        b[off    ] = (byte) (val >>> 24);
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写人的float型数据。
     * 首先通过Float.floatToIntBits方法将传入的float型数据转换成int型数据，然后调用对应的putInt方法，将数据写入到传入的字节数组当中。需要时，使用对应的
     * getFloat方法便可读取数据。
     */
    static void putFloat(byte[] b, int off, float val) {
        putInt(b, off,  Float.floatToIntBits(val));
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写人的int型数据。
     * 通过位运算分别将long型数据的64位数据，每八位一组，存放于传入的byte型数组之中。需要时，使用对应的getLong方法便可读取数据。
     */
    static void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) (val       );
        b[off + 6] = (byte) (val >>>  8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off    ] = (byte) (val >>> 56);
    }

    /*
     * 该方法提供3个参数，第一个为一个byte型数组，作为数据的缓存区，第二个为在数组中开始记录数据的起点位置，第三个为要写人的float型数据。
     * 首先通过Double.doubleToLongBits方法将传入的double型数据转换成long型数据，然后调用对应的putLong方法，将数据写入到传入的字节数组当中。需要时，使用对
     * 应的getDouble方法便可读取数据。
     */
    static void putDouble(byte[] b, int off, double val) {
        putLong(b, off, Double.doubleToLongBits(val));
    }
}

