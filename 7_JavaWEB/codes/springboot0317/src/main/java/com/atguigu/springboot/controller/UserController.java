package com.atguigu.springboot.controller;

import com.atguigu.springboot.beans.User;
import com.atguigu.springboot.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 控制层 : 用于处理客户端的请求，以及给客户端完成响应
 */

// @RestController 相当于 @Controller +    @ResponseBody
@Controller
public class UserController {

    @Autowired
    private UserService userService;

    /**
     * 客户端:  http://localhost:8888/springboot0317/regist
     *
     *  @RequestMapping("/regist") : 将来处理客户端的regist请求
     *
     *  方法的user参数用来接收客户端提交的参数，只需要保证客户端的参数名与user类中的属性名一致，就能直接将
     *  客户端的参数封装到User类中
     *
     *   @ResponseBody 将方法的返回值处理成json字符串响应给客户端
     *
     *
     */
    @ResponseBody
    @RequestMapping("/regist")
    public String  doRegist(User user){
        System.out.println("Controller User : " +user );
        userService.registuser(user);
        return "regist success" ;
    }
}
