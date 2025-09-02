package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        // 从redis查
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        R r = null;
        // 判断是否存在
        if(StrUtil.isNotBlank(json)) {
            // 存在则直接返回
            return JSONUtil.toBean(json, type);
        }

        // 由于穿透问题处理，此处增加判断命中的是不是空值
        if(Objects.nonNull(json)){
            // 返回错误信息
            return null;
        }

        // 不存在则查数据库
        r = dbFallback.apply(id);

        // 不存在于数据库则返回错误
        if(Objects.isNull(r)) {
            // 将空值写入redis，应对传统问题
            // TTL添加随机值防止雪崩
            this.set(key, "", RedisConstants.CACHE_NULL_TTL + new Random().nextInt(10), TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }
        // 存在于数据库则写回缓存，TTL添加随机值防止雪崩
        this.set(key, r, (time + new Random().nextInt(10)), unit);

        // 返回数据
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        // 从redis查
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isBlank(json)) {
            // 不存在，返回
            return null;
        }

        // 存在，首先序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 未过期直接返回
            return r;
        }

        // 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if(isLock){
            // 成功获取锁，开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }

        // 增加双检环节
        // 获取锁失败，再次查询缓存，判断缓存是否重建
        json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(json)) {
            // 不存在，返回
            return null;
        }

        // 存在，首先序列化
        redisData = JSONUtil.toBean(json, RedisData.class);
        r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        expireTime = redisData.getExpireTime();

        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 未过期直接返回
            return r;
        }

        // 返回过期数据
        return r;
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
}
