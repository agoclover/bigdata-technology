package com.atguigu.java;

/**
 * Author: Felix
 * Date: 2020/7/1
 * Desc: Java多态  --静态绑定和动态绑定
 *      -父类引用指向子类对象
 *      -只能调用其引用类型中定义的方法
 *          静态绑定也叫编译期绑定
 *              在编译阶段确定声明的对象类型中是否有该方法
 *              在编译阶段确定对象的属性是哪个类型的
 *      -在运行的时候，会执行子类中覆盖的方法
 *          动态绑定也叫运行期绑定
 *              在运行的时候确定对象的实际类型
 *
 *      -Java语言的属性是静态绑定，方法是动态绑定
 */
public class TestDynamic {
    public static void main(String[] args) {
        //Teacher tea = new Teacher();
        Person tea = new Teacher();
        System.out.println(tea.name);
        tea.hello();
        //tea.sayHi
    }
}
class Person {
    public String name = "person";
    public void hello() {
        System.out.println("hello person");
    }
}
class Teacher extends Person {
    public String name = "teacher";
    @Override
    public void hello() {
        System.out.println("hello teacher");
    }
    public void sayHi(){
        System.out.println("sayHI");
    }
}

