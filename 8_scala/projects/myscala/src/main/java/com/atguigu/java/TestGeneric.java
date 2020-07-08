package com.atguigu.java;

/**
 * Author: Felix
 * Date: 2020/7/4
 * Desc: Java泛型
 */
class Parent{}
class Child extends Parent{}
class Sub extends Child{}
//泛型模板
class Student<T>{}

public class TestGeneric {
    public static void main(String[] args) {
        //泛型的不可变性
        //Student<Child> s = new Student<Child>();
        //Student<Child> s1 = new Student<Parent>();
        //Student<Child> s2 = new Student<Sub>();
        test(Child.class);
        test(Parent.class);
        //test(Sub.class);
    }

    //?泛型通配符
    //泛型上界（上限）
    /*public static void test(Class<? extends Child> clz){
        System.out.println(clz.getName());
    }*/

    //泛型下界（下限）
    public static void test(Class<? super Child> clz){
        System.out.println(clz.getName());
    }
}
