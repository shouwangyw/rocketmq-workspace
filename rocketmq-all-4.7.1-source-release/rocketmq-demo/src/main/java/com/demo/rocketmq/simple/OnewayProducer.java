package com.demo.rocketmq.simple;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 简单消息：发送单向消息
 *
 * @Author 01399565
 * @create 2020/11/3 17:28
 */
public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("oneway-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();

        for (int i = 0; i < 10; i++) {
            val msg = new Message("simple_msg_topic", "oneway", "key_" + i,
                    ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送单向消息
            producer.sendOneway(msg);
        }
        System.out.println("Send finish");
        TimeUnit.SECONDS.sleep(5);
        producer.shutdown();
    }
}
