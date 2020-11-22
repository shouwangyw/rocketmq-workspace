package com.demo.rocketmq.simple;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 简单消息：消费消息
 *
 * @Author 01399565
 * @create 2020/11/3 17:39
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        // 实例化消息消费者Consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("simple-message-consumer-group");
        // 设置NameServer地址
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        // 订阅一个或多个Topic，以及tag来过滤需要消费的消息
        consumer.subscribe("simple_msg_topic", "*");
        // 注册回调实现类来处理从broker拉取回来的消息
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
