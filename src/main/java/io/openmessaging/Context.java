package io.openmessaging;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class Context {
    public int offsetA = 0;

    public int offsetB = 0;

    public int tag = 0;

    public int tagIndex = 0;

    public int offsetB32 = 0;

    public int pre32Max = 0;

    public int pre32Min = 0;

    public void clear() {
        offsetA = 0;
        offsetB = 0;
        tag = 0;
        tagIndex = 0;
        offsetB32 = 0;
        pre32Max = 0;
        pre32Min = 0;
    }
}
