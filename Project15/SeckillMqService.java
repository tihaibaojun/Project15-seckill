package org.example.vscode_3.Project15;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * MQ 异步秒杀服务 —— Redis + RocketMQ + PG 三件套融合版
 * 相比 Project10 的同步版 SeckillServices，这里最大的区别：
 *   下单请求不直接写 PG</b> —— Redis Lua 原子扣库存成功后，只发一条 RocketMQ 消息，立刻返回"排队中"</li>
 *   PG 写订单由 MQ 消费者异步完成</b> —— 削峰填谷，PG 只按消费者速度匀速写入</li>
 *   Redisson 锁移到消费者侧</b> —— 生产者不抢锁，锁的粒度更细（同一消息串行化）</li>
 * 四件事的分工：
 *   Spring Boot  → 编排入口 / 对外 REST API
 *   Redis        → ① 原子扣库存（Lua 脚本）② 防重复下单（Set 去重）③ 排队结果缓存
 *   RocketMQ     → 削峰：秒杀消息排队，消费者匀速落 PG
 *   PostgreSQL   → 订单持久化（唯一索引兜底防重复）
 * 流程：
 *   用户请求 1000 QPS
 *     ↓
 *   Redis Lua 原子扣库存 + Set 防重复（毫秒级，1000 QPS 轻松扛）
 *     ↓ 成功
 *   发 RocketMQ 消息
 *     ↓ 立刻返回 "排队中，请求ID=xxx"
 *     ↓
 *   RocketMQ Consumer（匀速消费，比如 100 msg/s）
 *     ↓
 *   Redisson 分布式锁（同一用户同一商品串行）
 *     ↓
 *   PG 写订单（唯一索引防重复兜底）
 *     ↓
 *   Redis 写结果（前端用轮询/长轮询查是否成功）
 */
@Slf4j
@Service
public class SeckillMqService {

    // ===== Redis key =====
    /** Redis 库存：seckill:stock:{productId} */
    public static final String STOCK_KEY   = "seckill:stock:";
    /** 已购用户集合：seckill:users:{productId} */
    public static final String USERS_KEY   = "seckill:users:";
    /** 排队结果：seckill:result:{requestId}，消费者处理完会写 SUCCESS/FAIL */
    public static final String RESULT_KEY  = "seckill:result:";

    // ===== RocketMQ =====
    /** 秒杀 Topic */
    public static final String SECKILL_TOPIC = "seckill-order";
    /** 秒杀 Tag */
    public static final String SECKILL_TAG   = "seckill";

    /**
     * Lua 脚本 —— 原子扣库存 + 防重复
     * 和 Project10 的 SeckillServices 里脚本完全一样。
     * Redis 单线程执行，原子不可分割，是防超卖的第一道防线。
     * 返回值约定：
     *   >= 0 ：剩余库存（成功）
     *   -1   ：售罄
     *   -2   ：该用户已抢过
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('sismember', KEYS[2], ARGV[1]) == 1 then return -2 end " +
                    "local stock = tonumber(redis.call('get', KEYS[1])) " +
                    "if stock == nil or stock <= 0 then return -1 end " +
                    "redis.call('sadd', KEYS[2], ARGV[1]) " +
                    "redis.call('decr', KEYS[1]) " +
                    "return stock - 1",
            Long.class);

    private final StringRedisTemplate redis;
    private final RocketMQTemplate rocketMQ;

    public SeckillMqService(StringRedisTemplate redis, RocketMQTemplate rocketMQ) {
        this.redis   = redis;
        this.rocketMQ = rocketMQ;
    }

    /**
     * 秒杀主入口 —— 先 Redis 拦截，再发 MQ
     * <p>
     * 注意：这里<b>不碰 PG</b>、<b>不抢 Redisson 锁</b>。
     * PG 和锁都留给消费者做，生产者只管两件事：
     *   ① Redis Lua 原子扣库存（拦掉 99% 无效请求）
     *   ② 发 RocketMQ 消息（削峰）
     *
     * @return 结果 Map：
     *   code  = 0  成功（已进 MQ 队列，等待消费者处理）
     *   code  = -1 售罄
     *   code  = -2 重复购买
     *   requestId = 唯一请求号（消费者处理完后可用来查结果）
     */
    public Map<String, Object> seckill(Long productId, Long userId) {
        String stockKey = STOCK_KEY + productId;
        String usersKey = USERS_KEY + productId;

        // ① 兜底：Redis 没库存就同步一份（防止 key 过期）
        // redis.hasKey 检查 key 是否存在
        if (!Boolean.TRUE.equals(redis.hasKey(stockKey))) {
            // 没有 product 表引用，这里默认按 0 处理让 Lua 脚本拒绝
            // redis.opsForValue().setIfAbsent() 作用：如果 key 不存在则设置参数2，否则不作任何操作
            redis.opsForValue().setIfAbsent(stockKey, "0");
        }

        // ② Lua 原子操作：一人一单 + Redis 扣库存
        Long result = redis.execute(SECKILL_SCRIPT,
                List.of(stockKey, usersKey),
                String.valueOf(userId));

        if (result == null) {
            return Map.of("code", -1, "msg", "系统异常，请重试");
        }
        if (result == -2L) {
            return Map.of("code", -2, "msg", "每人限购一件，您已经抢到过了");
        }
        if (result < 0) {
            return Map.of("code", -1, "msg", "手慢了，商品已被抢光！");
        }

        // ③ 生成唯一 requestId，发 MQ
        // UUID 唯一标识一次请求，消费者处理完后可用来查询结果
        String requestId = UUID.randomUUID().toString().replace("-", "");

        // 先把结果占位，避免消费者处理完之前前端查到空 key
        redis.opsForValue().set(RESULT_KEY + requestId, "PENDING", 60, TimeUnit.SECONDS);

        // Map.of() 作用是创建一个不可变的 Map
        Map<String, Object> msg = Map.of(
                "requestId", requestId,
                "productId", productId,
                "userId",    userId,
                "timestamp", System.currentTimeMillis()
        );

        try {
            // 发送到 RocketMQ（destination 格式：topic:tag）
            rocketMQ.convertAndSend(SECKILL_TOPIC + ":" + SECKILL_TAG, (Object) msg);
            log.info("秒杀消息已入队 → requestId={}, productId={}, userId={}", requestId, productId, userId);

            // Map.of()作用是 创建一个不可变的 Map
            // 返回值约定：code=0 表示请求已入队，正在排队处理
            return Map.of(
                    "code",      0,
                    "msg",       "秒杀请求已入队，正在排队处理",
                    "requestId", requestId,
                    "remainStock", result   // 扣完后 Redis 剩余库存
            );

        } catch (Exception e) {
            // MQ 发送失败 → 必须回补 Redis（刚才 Lua 已经扣了！）
            log.error("RocketMQ 发送失败，回补 Redis, productId={}, userId={}", productId, userId, e);
            rollbackRedis(stockKey, usersKey, userId);
            redis.delete(RESULT_KEY + requestId);
            return Map.of("code", -1, "msg", "系统繁忙，请重试");
        }
    }

    /** 查询排队结果 —— 前端轮询这个接口知道自己抢到没 */
    public String queryResult(String requestId) {
        String v = redis.opsForValue().get(RESULT_KEY + requestId);
        return v == null ? "NOT_FOUND" : v;
    }

    /**
     * 回补 Redis —— MQ 发失败时兜底
     * Lua 已经扣了库存、加了已购集合，回补回去让用户可以重试
     */
    public void rollbackRedis(String stockKey, String usersKey, Long userId) {
        redis.opsForValue().increment(stockKey);
        redis.opsForSet().remove(usersKey, String.valueOf(userId));
    }
}
