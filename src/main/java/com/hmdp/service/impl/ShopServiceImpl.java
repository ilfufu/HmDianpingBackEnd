package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id){
        // 存在缓存穿透问题的实现
        // 实现1
        // Shop shop = queryWithPassThrough(id);
        // 实现2
//         Shop shop = cacheClient.queryWithPassThrough(
//                RedisConstants.CACHE_SHOP_KEY,
//                id,
//                Shop.class,
//                this::getById,
//                RedisConstants.CACHE_SHOP_TTL,
//                TimeUnit.MINUTES);

        // 互斥锁解决缓存穿透
        Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存穿透，需要预热将数据先放入redis否则会报错
        // 实现1
        // Shop shop = queryWithLogicalExpire(id);
        // 实现2
//        Shop shop = cacheClient.queryWithLogicalExpire(
//                RedisConstants.CACHE_SHOP_KEY,
//                id,
//                Shop.class,
//                this::getById,
//                RedisConstants.CACHE_SHOP_TTL,
//                TimeUnit.MINUTES
//        );

        // 判断并返回数据
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

/*    public Shop queryWithPassThrough(Long id){
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
    }*/

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

/*    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        // 从redis查
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isBlank(shopJSON)) {
            // 不存在，返回
            return null;
        }

        // 存在，首先序列化
        RedisData redisData = JSONUtil.toBean(shopJSON, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 未过期直接返回
            return shop;
        }

        // 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if(isLock){
            // 成功获取锁，开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    saveShop2Redis(id, 20L);
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
        shopJSON = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(shopJSON)) {
            // 仍未命中
            return null;
        }

        // 序列化
        redisData = JSONUtil.toBean(shopJSON, RedisData.class);
        shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        expireTime = redisData.getExpireTime();

        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 未过期直接返回
            return shop;
        }

        // 返回过期数据
        return shop;
    }*/

    public void saveShop2Redis(Long id, Long expireSeconds){
        // 查询店铺数据
        Shop shop = getById(id);

        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // 写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
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

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y){
        // 注意：需要redis 6.0以上版本，由于版本原因此功能实现了未启用

        // 先判断是否需要坐标判断
        if(x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 查询redis
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 解析
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        // 截取from到end
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD( id, " + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 返回
        return Result.ok(shops);
    }

}
