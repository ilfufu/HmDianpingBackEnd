package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
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
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private IVoucherOrderService proxy;

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    // 获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                }
//                catch (Exception e){
//                    log.error("处理订单异常情况", e);
//                }
//            }
//        }
//    }

    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";

        @Override
        public void run() {
            while(true){
                try {
                    // 获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 判断消息是否获得成功
                    if(list == null || list.isEmpty()){
                        // 失败则下一次循环
                        continue;
                    }

                    // 解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 成功则创建订单
                    handleVoucherOrder(voucherOrder);

                    // ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                }
                catch (Exception e){
                    log.error("处理订单异常情况", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    // 获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 判断消息是否获得成功
                    if(list == null || list.isEmpty()){
                        // 失败结束
                        break;
                    }

                    // 解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 成功则创建订单
                    handleVoucherOrder(voucherOrder);

                    // ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                }
                catch (Exception e){
                    log.error("处理pending-list订单异常情况", e);
                    try {
                        Thread.sleep(20);
                    }catch (InterruptedException e1){
                        e1.printStackTrace();
                    }
                }
            }
        }

    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取用户id
        Long userId = voucherOrder.getUserId();

        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 尝试获取锁
        boolean isLock = lock.tryLock();

        // 获取失败
        if(!isLock){
            // 返回错误信息
            log.error("不允许重复下单");
            return;
        }

        // 加锁防止一人一单功能失效
        try {
            proxy.createVoucherOrder(voucherOrder);
        }
        finally {
            // 释放锁
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId){
        // ！注意：此处要正常使用需要先在redis创建名为stream.orders的stream用户组

        // 执行lua脚本
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                orderId.toString()
        );

        // 判断返回结果
        int r = result.intValue();

        // 没有购买资格
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId){
//        // 执行lua脚本
//        Long userId = UserHolder.getUser().getId();
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//
//        // 判断返回结果
//        int r = result.intValue();
//
//        // 没有购买资格
//        if(r != 0){
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        // 有购买资格，保存到阻塞队列
//        // 保存到阻塞队列
//        // 创建并保存订单信息
//        VoucherOrder voucherOrder = new VoucherOrder();
//
//        // 唯一ID
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//
//        // 用户ID
//        voucherOrder.setUserId(userId);
//
//        // 秒杀ID
//        voucherOrder.setVoucherId(voucherId);
//        orderTasks.add(voucherOrder);
//
//        // 获取代理对象（事务）
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 返回订单id
//        return Result.ok(orderId);
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId){
//        // 查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 查询是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀未开始");
//        }
//
//        // 查询是否结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已结束");
//        }
//
//        // 改进：一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        // 创建锁对象
//        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        // 尝试获取锁
//        // boolean isLock = lock.tryLock(1200);
//        boolean isLock = lock.tryLock();
//
//        // 获取失败
//        if(!isLock){
//            // 返回错误信息
//            return Result.fail("不允许重复下单");
//        }
//
//        // 加锁防止一人一单功能失效
//        // synchronized (userId.toString().intern()) {
//        try {
//            // 获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId, userId);
//        }
//        finally {
//            // 释放锁
//            lock.unlock();
//        }
//        // }
//    }

//    @Transactional
//    public Result createVoucherOrder(Long voucherId, Long userId){
//        // 查询订单
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        // 判断有无订单数据
//        if(count > 0){
//            // 已经买过了
//            return Result.fail("用户曾购买过");
//        }
//
//        // 尝试扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId)
//                // .eq("stock", voucher.getStock()) // 乐观锁判断
//                .gt("stock", 0) // 改进乐观锁逻辑，同时利用update原子性/MySQL行锁防止超卖
//                .update();
//
//        // 库存不足扣减失败
//        if(!success){
//            return Result.fail("库存不足");
//        }
//
//        // 创建并保存订单信息
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 唯一ID
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//
//        // 用户ID
//        voucherOrder.setUserId(userId);
//
//        // 秒杀ID
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//
//        return Result.ok(orderId);
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        // 获取用户id
        Long userId = voucherOrder.getUserId();

        // 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        // 判断有无订单数据
        if(count > 0){
            // 已经买过了
            log.error("已经买过了");
            return;
        }

        // 尝试扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder)
                // .eq("stock", voucher.getStock()) // 乐观锁判断
                .gt("stock", 0) // 改进乐观锁逻辑，同时利用update原子性/MySQL行锁防止超卖
                .update();

        // 库存不足扣减失败
        if(!success){
            log.error("库存不足");
            return;
        }

        // 保存订单
        save(voucherOrder);
    }
}
