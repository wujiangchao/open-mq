package com.open.openmq.client.common;

import java.util.Random;

public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();
    private final Random random = new Random();
    /**
     * 0111 1111 1111 1111 1111 1111 1111 1111
     */
    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    public int incrementAndGet() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            //带参的nextInt(int x)则会生成一个范围在0~x（不包含X）内的任意正整数
            //不带参数的nextInt()会生成所有有效的整数（包含正数，负数，0）
            index = random.nextInt();
            this.threadLocalIndex.set(index);
        }
        this.threadLocalIndex.set(++index);
        //将产生的随机数与0x7fffffff按位与运算，而0x7fffffff换成二进制是31个1，由此可知，这句话的作用是将产生的随机数最高位置为0，其它位保持不变。
        return Math.abs(index & POSITIVE_MASK);
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
                "threadLocalIndex=" + threadLocalIndex.get() +
                '}';
    }
}
