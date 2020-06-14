package com.atguigu.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * 基于Tomcat对Servlet规范的实现， 处理客户端的请求.
 *
 * Sun制定了Servlet规范, Tomcat实现了Servlet规范.
 */
public class RegistServlet  extends HttpServlet {

    /**
     * 用于处理客户端的post方式的请求
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPost(req, resp);
    }
     */

    /**
     * 用户处理客户端的get方式的请求
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doGet(req, resp);
    }*/

    /**
     * 重写HttpServlet中service的逻辑， 在方法中处理所有的请求
     * @param req  请求对象，
     * @param resp 响应对象
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        //1. 处理请求和响应的乱码问题
        req.setCharacterEncoding("utf-8");
        resp.setContentType("text/html;charset=utf-8");
        PrintWriter writer = resp.getWriter();
        //2. 获取客户端提交的数据
        String username = req.getParameter("username");
        String password = req.getParameter("password");

        //3. 数据的校验

        //4. 数据失败， 注册请求打回. 数据校验成功，继续后续的处理.
        if("".equals(username) || "".equals(password)){
            writer.println("REGIST  FAIL");
            writer.close();
            return ;
        }

        //5. 将校验成功的数据，写到数据库中.
        // JDBC
        System.out.println("REGIST SUCCESS: " + username + " , " + password);

        //6. 给客户端进行响应
        writer.println("REGIST  SUCCESS");
        writer.close();
    }
}
