package org.example.vscode_3.Project15;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.vscode_3.Project09.ProducMapper;
import org.example.vscode_3.Project09.Product;
import org.example.vscode_3.Project10.Order;
import org.example.vscode_3.Project10.OrderMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 秒杀下单事务服务 —— 数据库事务 + 消费幂等的落地点
 * 为什么单独抽一个 Service，而不是直接写在消费者里？
 *   {@code @Transactional} 依赖 Spring AOP 代理，类内部自调用不会走代理，事务会失效
 *   MQ 监听器只负责"收消息、写结果"，数据库事务交给这里，职责更清晰
 *   一个事务里做三件事（要么全成功，要么全回滚）：
 *   ① 幂等检查：request_id 已存在则直接返回原订单（重复消息不重复下单）
 *   ② 扣减 PG 库存：where stock > 0，扣不到说明库存不足 → 抛异常回滚
 *   ③ 写入订单：request_id 唯一索引作为最后一道防线
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SeckillOrderTxService {

    private final OrderMapper  orderMapper;
    private final ProducMapper productMapper;

    /**
     * 在一个数据库事务里完成：幂等检查 + 扣库存 + 写订单
     *
     * @param productId 商品 ID
     * @param userId    用户 ID
     * @param requestId 秒杀请求唯一标识（同一条消息重复投递时保持不变）
     * @return 订单实体（首次写入的新订单，或幂等命中时已存在的订单）
     * @throws SeckillBusinessException 库存不足等终态业务失败（会触发回滚）
     */
    @Transactional(rollbackFor = Exception.class)
    public Order createOrder(Long productId, Long userId, String requestId) {

        // 消费幂等：同一 requestId 已经处理过，直接返回原订单，不再重复扣库存/写订单
        Order existing = orderMapper.selectOne(
                new LambdaQueryWrapper<Order>().eq(Order::getRequestId, requestId));
        if (existing != null) {
            log.info("幂等命中，跳过重复处理 → requestId={}, orderId={}", requestId, existing.getId());
            // 幂等命中，跳过重复处理，后面的代码不再执行
            return existing;
        }

        // 扣减 PG 库存：where stock > 0，并发下也不会扣成负数
        UpdateWrapper<Product> stockWrapper = new UpdateWrapper<>();
        stockWrapper.eq("id", productId)
                .gt("stock", 0)
                .setSql("stock = stock - 1");
        // productMapper.update(参数1,参数2)作用：更新数据，参数1为实体类，参数2为更新条件，返回值为受影响行数
        int rows = productMapper.update(null, stockWrapper);
        if (rows == 0) {
            // 扣不到库存 → 抛出终态业务异常，同时触发本事务回滚
            throw new SeckillBusinessException("PG 库存不足");
        }

        // 写入订单
        Product product = productMapper.selectById(productId);
        Order order = new Order();
        order.setUserId(userId);
        order.setProductId(productId);
        order.setRequestId(requestId);
        order.setOrderPrice(product == null || product.getPrice() == null
                ? BigDecimal.ZERO
                : product.getPrice());
        order.setCreateTime(LocalDateTime.now());
        orderMapper.insert(order);

        return order;
    }
}
