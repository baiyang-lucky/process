package com.tbox.process.type;

/**
 * 五素元组
 *
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public final class Tuple5<T1, T2, T3, T4, T5> {
    public final T1 t1;
    public final T2 t2;
    public final T3 t3;
    public final T4 t4;
    public final T5 t5;

    public Tuple5(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
        this.t1 = t1;
        this.t2 = t2;
        this.t3 = t3;
        this.t4 = t4;
        this.t5 = t5;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s,%s,%s,%s)", t1, t2, t3, t4, t5);
    }
}
