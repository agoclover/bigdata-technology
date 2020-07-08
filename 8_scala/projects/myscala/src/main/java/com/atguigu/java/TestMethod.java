package com.atguigu.java;

/**
 * Author: Felix
 * Date: 2020/6/30
 * Desc:
 */
public class TestMethod {
    public void m1(){
        int a = 10;
        m2(a);
    }
    public void m2(int a){
        m3(a);
    }
    public void m3(int a){
        System.out.println(a);
    }
}
