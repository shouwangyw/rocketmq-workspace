package com.demo.rocketmq.transaction;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务消息消费者
 *
 * @Author 01399565
 * @create 2020/11/4 15:30
 */
public class TransactionConsumer {
    public static void main(String[] args) throws Exception {
        // 创建消息消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction-message-consumer-group");
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        // 订阅
        consumer.subscribe("transaction_msg_topic", "*");
        // 设置回调函数，处理消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer Started.");
    }
}
