package com.atguigu.java;

/**
 * Author: Felix
 * Date: 2020/6/29
 * Desc:
 */
public class TestEquals {
    public static void main(String[] args) {
        String s1 = new String("abc");
        String s2 = new String("abc");
        //在Java语言中，==比较的是内存地址
        System.out.println(s1 == s2); //false
        //在Java语言中，equals默认比较的也是内存地址，因为在String中对Object类中的equals进行了重写
        System.out.println(s1.equals(s2)); //true

    }
}
