package com.demo.rocketmq.batch;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

/**
 * 批量消息消费者
 *
 * @Author 01399565
 * @create 2020/11/4 14:43
 */
public class BatchConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batch-message-consumer-group");
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        consumer.subscribe("batch_msg_topic", "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer Started");
    }
}
