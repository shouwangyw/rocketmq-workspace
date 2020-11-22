package com.demo.rocketmq.filter;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 过滤消息消费者：通过Sql进行过滤
 *
 * @Author 01399565
 * @create 2020/11/4 14:53
 */
public class SqlFilterConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("filter-message-consumer-group");
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        // 使用SQL表达式进行消息过滤
        consumer.subscribe("filter_msg_topic", MessageSelector.bySql("age between 1 and 6"));

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("%s Receive New Message: %s %n",
                        Thread.currentThread().getName(), new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer Started.");
    }
}
