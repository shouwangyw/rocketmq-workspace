package com.demo.rocketmq.broadcasting;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * 广播消息消费者
 *
 * @Author 01399565
 * @create 2020/11/4 10:54
 */
public class BroadcastingConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcasting-message-consumer-group");
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("broadcasting_msg_topic", "*");
//        // 设置消息模式为：广播消费模式
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 设置消息模式为：集群消费模式
        consumer.setMessageModel(MessageModel.CLUSTERING);

        // MessageListenerConcurrently
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer started");
    }
}
