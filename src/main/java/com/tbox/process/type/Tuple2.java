package com.tbox.process.type;

/**
 * 二元素元组
 *
 * @param <T1>
 * @param <T2>
 * @author 白杨
 * DateTime:2023/8/22 10:43
 */
public final class Tuple2<T1, T2> {
    public final T1 t1;
    public final T2 t2;

    public Tuple2(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", t1, t2);
    }
}
