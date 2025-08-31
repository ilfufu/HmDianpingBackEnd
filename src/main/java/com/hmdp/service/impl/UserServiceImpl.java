package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Override
    public Result sendCode(String phone, HttpSession session){
        // 校验手机号格式
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 无效报错
            return Result.fail("手机号格式错误");
        }

        // 生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 保存验证码到session
        session.setAttribute("code", code);

        // 发送验证码，此处不实现
        log.debug("发送验证码成功，验证码：");
        log.debug(code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session){
        // 校验手机号格式
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 无效报错
            return Result.fail("手机号格式错误");
        }

        // 校验验证码
        Object cachedCode = session.getAttribute("code");
        String code = loginForm.getCode();
        if(code == null || !code.equals(cachedCode.toString())){
            // 验证码有误
            return Result.fail("验证码错误");
        }

        // 根据手机号查用户
        User user = query().eq("phone", phone).one();

        // 判断用户是否存在
        if(user == null){
            // 不存在，创建新用户
            user = createWithPhone(phone);
        }

        // 保存用户到session
        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));

        return Result.ok();
    }

    private User createWithPhone(String phone){
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomNumbers(10));
        save(user);
        return user;
    }
}
