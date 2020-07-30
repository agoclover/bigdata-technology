package com.atguigu.myspark.day01;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/21 10:36 上午
 */
public class TestMy {
    public static void main2(String[] args) throws ExecutionException, InterruptedException {
//        FutureTask;
        MyRunnable myRunnable = new MyRunnable();

        FutureTask<Integer> task = new FutureTask<>(myRunnable, myRunnable.getSum());

        Thread thread = new Thread(task);
        thread.start();

        Integer sum = task.get();

        Thread.sleep(5000);

        Integer sum1 = task.get();

        System.out.println(sum);
        System.out.println(sum1);

    }

    public static void main(String[] args) {
        MyRunnable2 myRunnable2 = new MyRunnable2();

        Exception e = null;

        FutureTask<Object> task = new FutureTask<>(myRunnable2, e);

        Thread thread = new Thread(task);
        thread.start();

        try {
            Object o = task.get();
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        } catch (ExecutionException executionException) {
            executionException.printStackTrace();
        }
    }
}
class MyRunnable2 implements Runnable{
    @Override
    public void run() {
        throw new RuntimeException("test");
    }
}


class MyRunnable implements Runnable{
    private int sum = 0;
    @Override
    public void run() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10; i++) {
            sum += i;
        }
        System.out.println(sum);
    }

    public Integer getSum(){
        return sum;
    }
}
