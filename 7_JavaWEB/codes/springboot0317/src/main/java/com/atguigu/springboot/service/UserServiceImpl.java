package com.atguigu.springboot.service;

import com.atguigu.springboot.beans.User;
import com.atguigu.springboot.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserServiceImpl implements  UserService {

    @Autowired   // SpringBoot会将UserMapper对象注入进来
    private UserMapper userMapper ;

    @Override
    public int registuser(User user) {
        return userMapper.insert(user);   // 把数据写入到数据库
    }
}
