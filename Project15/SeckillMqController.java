package org.example.vscode_3.Project15;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.vscode_3.Project09.ProducMapper;
import org.example.vscode_3.Project09.Product;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MQ 异步秒杀 Controller —— 对外 REST API
 * <p>
 * 三个接口：
 * <ol>
 *   <li><b>POST /seckill/mq</b>         —— 发起秒杀，Redis Lua 拦截 → RocketMQ 入队</li>
 *   <li><b>GET  /seckill/result</b>      —— 前端轮询查询秒杀结果（排队中/成功/失败）</li>
 *   <li><b>GET  /seckill/initRedis</b>   —— 秒杀前调用，把 PG 库存全量同步到 Redis</li>
 * </ol>
 */
@Slf4j
@RestController
@RequestMapping("/seckill")
@RequiredArgsConstructor
public class SeckillMqController {

    private final SeckillMqService   seckillMqService;
    private final ProducMapper       productMapper;
    private final StringRedisTemplate redis;

    /**
     * ① 秒杀入口
     * <p>
     * curl -X POST "http://localhost:8072/seckill/mq?productId=1&userId=1001"
     */
    @PostMapping("/mq")
    public Map<String, Object> seckill(@RequestParam Long productId,
                                       @RequestParam Long userId) {
        log.info("秒杀请求 → productId={}, userId={}", productId, userId);
        return seckillMqService.seckill(productId, userId);
    }

    /**
     * ② 查询排队结果 —— 前端拿到 requestId 后轮询这个接口
     * <p>
     * curl "http://localhost:8072/seckill/result?requestId=xxx"
     * <p>
     * 返回值：
     * <ul>
     *   <li>PENDING      —— 还在排队，消费者没处理完</li>
     *   <li>SUCCESS:N    —— 秒杀成功，N 是订单 ID</li>
     *   <li>FAIL:xxx     —— 秒杀失败（原因）</li>
     *   <li>NOT_FOUND    —— requestId 不存在或已过期</li>
     * </ul>
     */
    @GetMapping("/result")
    public Map<String, Object> result(@RequestParam String requestId) {
        String v = seckillMqService.queryResult(requestId);
        Map<String, Object> r = new HashMap<>();
        r.put("requestId", requestId);
        r.put("status", v);
        // 给前端一个友好的 code 字段
        if ("NOT_FOUND".equals(v)) {
            r.put("code", -1);
            r.put("msg", "❌ 请求不存在或已过期");
        } else if (v.startsWith("SUCCESS")) {
            r.put("code", 0);
            r.put("msg", "✅ 秒杀成功！");
        } else if (v.startsWith("FAIL")) {
            r.put("code", -1);
            r.put("msg", "❌ 秒杀失败：" + v);
        } else {
            r.put("code", 1);
            r.put("msg", "⏳ 排队中，请稍候再查...");
        }
        return r;
    }

    /**
     * ④ 商品列表 —— 页面展示用，同时返回 PG 库存和 Redis 当前库存
     * <p>
     * curl "http://localhost:8072/seckill/products"
     */
    @GetMapping("/products")
    public List<Map<String, Object>> products() {
        return productMapper.selectList(null).stream().map(p -> {
            String redisStock = redis.opsForValue().get(SeckillMqService.STOCK_KEY + p.getId());
            Map<String, Object> m = new HashMap<>();
            m.put("id", p.getId());
            m.put("name", p.getName());
            m.put("price", p.getPrice());
            m.put("dbStock", p.getStock() == null ? 0 : p.getStock());
            m.put("redisStock", redisStock == null ? null : Integer.valueOf(redisStock));
            return m;
        }).toList();
    }

    /**
     * ③ 秒杀前初始化 Redis —— 从 PG 拉所有商品库存
     * <p>
     * curl "http://localhost:8072/seckill/initRedis"
     */
    @GetMapping("/initRedis")
    public Map<String, Object> initRedis() {
        List<Product> products = productMapper.selectList(null);
        int count = 0;
        for (Product p : products) {
            String stockKey = SeckillMqService.STOCK_KEY + p.getId();
            String usersKey = SeckillMqService.USERS_KEY + p.getId();
            // 库存 → 写 Redis
            redis.opsForValue().set(stockKey, String.valueOf(p.getStock() == null ? 0 : p.getStock()));
            // 已购集合 → 清空（新一轮秒杀重置）
            redis.delete(usersKey);
            count++;
            log.info("初始化 Redis → productId={}, stock={}", p.getId(), p.getStock());
        }
        Map<String, Object> r = new HashMap<>();
        r.put("code", 0);
        r.put("msg", "✅ Redis 初始化完成");
        r.put("productCount", count);
        r.put("details", products.stream().map(p -> Map.of(
                "productId", p.getId(),
                "name",      p.getName(),
                "stock",     p.getStock()
        )).toList());
        return r;
    }
}
