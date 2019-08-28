package io.openmessaging;

import java.util.Objects;

/**
 * Created by huzhebin on 2019/07/27.
 */
public class Pair<A, B> {
    public A fst;
    public B snd;

    public Pair(A var1, B var2) {
        this.fst = var1;
        this.snd = var2;
    }

}
