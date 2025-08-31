package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import org.springframework.beans.BeanUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 获取session
        HttpSession session = request.getSession();

        // 获取user信息
        Object user = session.getAttribute("user");

        // 判断user是否存在
        if (user == null) {
            // 不存在，拦截
            response.setStatus(401);
            return false;
        }

        // 存在，保存到ThreadLocal
        UserHolder.saveUser((UserDTO) user);

        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
