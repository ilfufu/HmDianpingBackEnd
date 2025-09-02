package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
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
        // 存在缓存穿透可能的实现
        // Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存穿透
        Shop shop = queryWithMutex(id);

        // 判断并返回数据
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    public Shop queryWithPassThrough(Long id){
        // 从redis查
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isNotBlank(shopJSON)) {
            // 存在则直接返回
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return shop;
        }

        // 由于穿透问题处理，此处增加判断命中的是不是空值
        if(shopJSON != null){
            // 返回错误信息
            return null;
        }

        // 不存在则查数据库
        Shop shop = getById(id);

        // 不存在于数据库则返回错误
        if(shop == null) {
            // 将空值写入redis，应对传统问题
            // TTL添加随机值防止雪崩
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL + new Random().nextInt(10), TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }
        // 存在于数据库则写回缓存，TTL添加随机值防止雪崩
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL + new Random().nextInt(10), TimeUnit.MINUTES);

        // 返回数据
        return shop;
    }

    public Shop queryWithMutex(Long id){
        // 从redis查
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isNotBlank(shopJSON)) {
            // 存在则直接返回
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return shop;
        }

        // 由于穿透问题处理，此处增加判断命中的是不是空值
        if(shopJSON != null){
            // 返回错误信息
            return null;
        }

        // 获取互斥锁并判断是否成功
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);

//             // 判断是否加锁成功，递归
//            if (!isLock) {
//                // 失败则休眠并重试
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }

            // 判断是否加锁成功，循环
            while (!isLock) {
                // 失败则休眠并重试
                Thread.sleep(50);
                isLock = tryLock(lockKey);
            }

            // 不存在则查数据库
            shop = getById(id);

            // 模拟重建延时
            Thread.sleep(200);

            // 不存在于数据库则返回错误
            if (shop == null) {
                // 将空值写入redis，应对传统问题
                // TTL添加随机值防止雪崩
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL + new Random().nextInt(10), TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }
            // 存在于数据库则写回缓存，TTL添加随机值防止雪崩
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL + new Random().nextInt(10), TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            // 释放锁
            unlock(lockKey);
        }

        // 返回数据
        return shop;
    }

    // 尝试加锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 解锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
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
