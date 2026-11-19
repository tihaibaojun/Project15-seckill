package org.example.vscode_3.Project15;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.example.vscode_3.Project10.Order;
import org.example.vscode_3.Project10.OrderMapper;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀订单消费者 —— 异步写 PG 的地方
 * 消费者只做三件事：
 *   Redisson 分布式锁 —— 同一用户同一商品的消息串行化，减少无谓并发
 *   调用 {@link SeckillOrderTxService} —— 真正的事务 + 幂等都在那里完成
 *   把处理结果写回 Redis—— 前端轮询 /seckill/result 用
 * 异常处理的关键区分：
 *   业务性失败（库存不足、重复购买）→ 写 FAIL 结果并正常返回，不让 MQ 重试
 *   系统异常（数据库连不上、抢不到锁）→ 抛出，交给 RocketMQ 自动重试
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = SeckillMqService.SECKILL_TOPIC,
        consumerGroup = "seckill-order-consumer",
        selectorExpression = SeckillMqService.SECKILL_TAG
)
public class SeckillOrderConsumer implements RocketMQListener<Map<String, Object>> {

    private final RedissonClient        redisson;
    private final StringRedisTemplate   redis;
    private final OrderMapper           orderMapper;
    private final SeckillOrderTxService orderTxService;

    @Override
    public void onMessage(Map<String, Object> msg) {
        String requestId = String.valueOf(msg.get("requestId"));
        Long   productId = toLong(msg.get("productId"));
        Long   userId    = toLong(msg.get("userId"));

        log.info("收到秒杀消息 → requestId={}, productId={}, userId={}", requestId, productId, userId);

        // Redisson 分布式锁：同一用户同一商品串行处理
        RLock lock = redisson.getLock("lock:seckill:mq:" + userId + ":" + productId);
        try {
            if (!lock.tryLock(2, 10, TimeUnit.SECONDS)) {
                // 抢不到锁 —— 另一实例正在处理，抛异常让 RocketMQ 稍后重投
                throw new RuntimeException("抢不到锁，稍后重试");
            }
        }
        // InterruptedException作用是捕获线程中断异常
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("被中断", e);
        }

        try {
            // 事务 + 幂等：扣 PG 库存、写订单，两步要么都成功要么都回滚
            Order order = orderTxService.createOrder(productId, userId, requestId);

            // 写成功结果到 Redis
            redis.opsForValue().set(
                    SeckillMqService.RESULT_KEY + requestId,
                    "SUCCESS:" + order.getId(),
                    60, TimeUnit.SECONDS
            );
            log.info("秒杀完成 ✅ → requestId={}, orderId={}", requestId, order.getId());

        }
        // SeckillBusinessException作用是捕获业务异常
        catch (SeckillBusinessException e) {
            // 终态业务失败（库存不足等）：重试也不会成功，写 FAIL 后正常返回
            log.warn("秒杀业务失败，不重试 → requestId={}, reason={}", requestId, e.getMessage());
            writeFail(requestId, e.getMessage());

        }
        // DataIntegrityViolationException作用是捕获唯一索引冲突异常
        catch (DataIntegrityViolationException e) {
            // 唯一索引 (user_id, product_id) 命中：说明该用户已经有订单了
            Order exist = orderMapper.selectOne(new LambdaQueryWrapper<Order>()
                    .eq(Order::getUserId, userId)
                    .eq(Order::getProductId, productId));

            if (exist == null) {
                // 查不到订单，情况不明，交给 MQ 重试
                throw e;
            }
            // 唯一索引冲突：说明该用户已经下过订单了
            if (requestId.equals(exist.getRequestId())) {
                // 同一条消息重复投递，第一次其实已经成功 → 按幂等返回成功结果
                redis.opsForValue().set(
                        SeckillMqService.RESULT_KEY + requestId,
                        "SUCCESS:" + exist.getId(),
                        60, TimeUnit.SECONDS
                );
                log.info("重复消息命中幂等，返回原订单 → requestId={}, orderId={}", requestId, exist.getId());
            } else {
                // 不同 requestId 但用户已买过 → 重复购买
                writeFail(requestId, "重复购买");
                log.warn("用户已购买过，拒绝重复下单 → productId={}, userId={}", productId, userId);
            }
        }
        // Exception作用是捕获其他异常
        catch (Exception e) {
            // 其他系统异常 → 抛出，让 RocketMQ 自动重试
            log.error("秒杀消费失败 → requestId={}, productId={}, userId={}", requestId, productId, userId, e);
            throw e;

        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void writeFail(String requestId, String reason) {
        redis.opsForValue().set(
                SeckillMqService.RESULT_KEY + requestId,
                "FAIL:" + reason,
                60, TimeUnit.SECONDS
        );
    }

    private Long toLong(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(o));
    }
}
