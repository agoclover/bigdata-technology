package com.atguigu.java;

/**
 * Author: Felix
 * Date: 2020/6/29
 * Desc: 运算符
 *      ++和--
 */
public class TestOperator {
    public static void main(String[] args) {
        //System.out.println(factorial(5));
        //1.++在前，先运算再赋值    ； ++在后，先赋值再运算
        //int x = 10;
        //int y = ++ x;
        //System.out.println("x="+ x +",y=" + y); //x = 11    y =11
        //int x = 10;
        //int y = x ++;
        //System.out.println("x="+ x +",y=" + y); //x = 11    y =10

        //2. x = x++  将等号右边的值赋值给等号的左边
        //第一步：  定义临时变量接收等号右边的计算结果  tmp = x ++    tmp = 10
        //第二步：x自增       x = 11
        //第三步：赋值，将等号右边的值赋值给等号左边   x = tmp =10
        //int x = 10;
        ////x = ++x;
        //x =  x ++;
        //System.out.println(x);

        byte b = 10;
        //b++;
        //++b;
        //b = b + 1
        //b +=1; //==> b = b + 1   +=底层会对结果进行强转
        System.out.println(b);
    }
   /* //定义一个方法，通过递归的方式，求一个整数的阶乘
    public static int factorial(int n){
        if(n==1){
            return 1;
        }
        return n * factorial(n-1);
    }*/

}
