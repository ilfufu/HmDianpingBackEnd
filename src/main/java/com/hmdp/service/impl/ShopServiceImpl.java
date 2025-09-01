package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Random;
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
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id){
        // 从redis查
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isNotBlank(shopJSON)) {
            // 存在则直接返回
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return Result.ok(shop);
        }

        // 由于穿透问题处理，此处增加判断命中的是不是空值
        if(shopJSON != null){
            // 返回错误信息
            return Result.fail("店铺信息不存在");
        }

        // 不存在则查数据库
        Shop shop = getById(id);

        // 不存在于数据库则返回错误
        if(shop == null) {
            // 将空值写入redis，应对传统问题
            // TTL添加随机值防止雪崩
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_SHOP_TTL + new Random().nextInt(10), TimeUnit.MINUTES);
            // 返回错误信息
            return Result.fail("店铺不存在");
        }
        // 存在于数据库则写回缓存，TTL添加随机值防止雪崩
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL + new Random().nextInt(10), TimeUnit.MINUTES);

        // 返回数据
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop){
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }

        // 更新数据库
        updateById(shop);

        // 删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
