package io.openmessaging;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by huzhebin on 2019/08/25.
 */
public class UnsafeWrapper {
    public static final Unsafe unsafe;

    static {
        Unsafe theUnsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            theUnsafe = null;
        }
        unsafe = theUnsafe;
    }
}
