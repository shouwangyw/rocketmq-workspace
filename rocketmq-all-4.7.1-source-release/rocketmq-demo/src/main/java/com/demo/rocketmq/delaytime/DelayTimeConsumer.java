package com.demo.rocketmq.delaytime;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 延时消息消费者
 *
 * @Author 01399565
 * @create 2020/11/4 11:13
 */
public class DelayTimeConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delaytime-message-consumer-group");
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("delaytime_msg_topic", "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("Receive message[msgId=%s] delay %d ms %n", msg.getMsgId(),
                        // 消息创建时间 - 消息存储到broker的时间 = 消息延时时间
                        msg.getBornTimestamp() - msg.getStoreTimestamp());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer started");
    }
}
