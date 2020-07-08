package com.atguigu.java;

/**
 * Author: Felix
 * Date: 2020/7/1
 * Desc:
 */
public class TestSuper {
    public static void main(String[] args) {
        BBB b = new BBB();
        b.m1();
    }
}

class AAA{
    String name = "zhangsan";
}
class BBB extends AAA{
    public void m1(){
        System.out.println(this);
        //System.out.println(super.name);
    }
}