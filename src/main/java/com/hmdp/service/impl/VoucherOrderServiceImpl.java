package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

//    @Autowired
//    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result seckillVoucher(Long voucherId){
        // 查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        // 查询是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            return Result.fail("秒杀未开始");
        }

        // 查询是否结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已结束");
        }

        // 改进：一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 尝试获取锁
        // boolean isLock = lock.tryLock(1200);
        boolean isLock = lock.tryLock();

        // 获取失败
        if(!isLock){
            // 返回错误信息
            return Result.fail("不允许重复下单");
        }

        // 加锁防止一人一单功能失效
        // synchronized (userId.toString().intern()) {
        try {
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId, userId);
        }
        finally {
            // 释放锁
            lock.unlock();
        }
        // }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId, Long userId){
        // 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 判断有无订单数据
        if(count > 0){
            // 已经买过了
            return Result.fail("用户曾购买过");
        }

        // 尝试扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                // .eq("stock", voucher.getStock()) // 乐观锁判断
                .gt("stock", 0) // 改进乐观锁逻辑，同时利用update原子性/MySQL行锁防止超卖
                .update();

        // 库存不足扣减失败
        if(!success){
            return Result.fail("库存不足");
        }

        // 创建并保存订单信息
        VoucherOrder voucherOrder = new VoucherOrder();
        // 唯一ID
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);

        // 用户ID
        voucherOrder.setUserId(userId);

        // 秒杀ID
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }
}
