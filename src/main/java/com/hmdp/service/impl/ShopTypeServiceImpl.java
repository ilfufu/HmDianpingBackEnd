package com.hmdp.service.impl;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList(){
        // 从redis查
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY + UUID.randomUUID().toString(true);
        String shopTypeJSON = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isNotBlank(shopTypeJSON)) {
            // 存在则直接返回
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJSON, ShopType.class);
            return Result.ok(shopTypes);
        }

        // 不存在则查数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        // 不存在于数据库则返回错误
        if(shopTypes == null) {
            return Result.fail("店铺类型不存在");
        }
        // 存在于数据库则写回缓存
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypes));
        stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

        // 返回数据
        return Result.ok(shopTypes);
    }
}
