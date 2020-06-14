package com.atguigu.springboot.springboot0317;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import tk.mybatis.spring.annotation.MapperScan;

/**
 * SpringBoot项目的启动类
 * @ComponentScan(value="com.atguigu") 会扫描com.atguigu包下及子包下所有的类,将带有注解的类管理到Spring的容器中
 *
 */
@MapperScan(basePackages = "com.atguigu.springboot.mapper")
@ComponentScan(value="com.atguigu")
@SpringBootApplication
public class Springboot0317Application {

    public static void main(String[] args) {
        SpringApplication.run(Springboot0317Application.class, args);
    }
}
