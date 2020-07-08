package com.atguigu.json;

import com.atguigu.bean.User;
import com.google.gson.Gson;
import org.junit.Test;

import java.util.ArrayList;

public class TestJson {

    @Test
    public void testJavaToJson(){
        User user = new User();
        user.setUsername("fangfang");
        user.setPassword("123456");
        Gson gson = new Gson() ;
        String jsonStr = gson.toJson(user);
        System.out.println(jsonStr);


        ArrayList  users = new ArrayList();
        User user1 = new User();
        user1.setUsername("kangkang");
        user1.setPassword("654321");
        users.add(user);
        users.add(user1);

        String listJsonStr = gson.toJson(users);
        System.out.println(listJsonStr);
    }

    @Test
    public void testJsonToJava(){
        String jsonStr = "{\"username\":\"fangfang\",\"password\":\"123456\"}" ;
        Gson gson  = new Gson();

        User user = gson.fromJson(jsonStr, User.class);
        System.out.println(user);
    }
}


