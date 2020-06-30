package com.atguigu.java;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/28 4:28 下午
 */
public class TestArray {
    public static void main(String[] args) {
        final int[] a = new int[]{1,2,3,4,5};
        a[2] = 100;
        System.out.println(a[2]);

        int x = 10;
        int y;
//        x = x++ + x++;
        y = x++ + x++;
        System.out.println(x);
        System.out.println(y);

        byte aa = (byte) 0b10010001;
        System.out.println(aa);
    }
}
