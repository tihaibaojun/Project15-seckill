package org.example.vscode_3.Project15;

/**
 * 秒杀业务性失败 —— 表示"再重试也不会成功"的终态错误
 * 和普通异常的区别：
 *   普通异常（数据库连接失败、抢锁失败等）—— 抛给 RocketMQ，让消息重投再试</li>
 *   本异常（库存不足、商品不存在等）—— 消费者捕获后直接写 FAIL 结果并正常返回，
 *       不要让 RocketMQ 白白重试 16 次
 * 同时它也是 {@link RuntimeException}，在 {@code @Transactional} 方法里抛出会触发数据库回滚。
 */

// 秒杀业务性失败 —— 表示"再重试也不会成功"的终态错误
// RuntimeException作用：让 Spring 事务回滚
public class SeckillBusinessException extends RuntimeException {

    public SeckillBusinessException(String message) {
        super(message);
    }
}
