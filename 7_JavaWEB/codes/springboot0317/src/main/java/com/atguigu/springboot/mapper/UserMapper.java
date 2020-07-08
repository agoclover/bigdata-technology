package com.atguigu.springboot.mapper;

import com.atguigu.springboot.beans.User;
import tk.mybatis.mapper.common.Mapper;

/**
 * 持久层: 主要负责与数据库交互，完成数据的CRUD
 *
 *
 * Mapper : 通用Mapper中提供的接口。 已经提供了基本的CRUD操作.
 *
 */
public interface UserMapper extends Mapper<User> {

}
