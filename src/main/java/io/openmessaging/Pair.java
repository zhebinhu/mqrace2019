package io.openmessaging;

import java.util.Objects;

/**
 * Created by huzhebin on 2019/07/27.
 */
public class Pair<A, B> {
    public final A fst;
    public final B snd;

    public Pair(A var1, B var2) {
        this.fst = var1;
        this.snd = var2;
    }

    public String toString() {
        return "Pair[" + this.fst + "," + this.snd + "]";
    }

    public boolean equals(Object var1) {
        return var1 instanceof com.sun.tools.javac.util.Pair && Objects.equals(this.fst, ((com.sun.tools.javac.util.Pair)var1).fst) && Objects.equals(this.snd, ((com.sun.tools.javac.util.Pair)var1).snd);
    }

    public int hashCode() {
        if (this.fst == null) {
            return this.snd == null ? 0 : this.snd.hashCode() + 1;
        } else {
            return this.snd == null ? this.fst.hashCode() + 2 : this.fst.hashCode() * 17 + this.snd.hashCode();
        }
    }

    public static <A, B> com.sun.tools.javac.util.Pair<A, B> of(A var0, B var1) {
        return new com.sun.tools.javac.util.Pair(var0, var1);
    }
}
