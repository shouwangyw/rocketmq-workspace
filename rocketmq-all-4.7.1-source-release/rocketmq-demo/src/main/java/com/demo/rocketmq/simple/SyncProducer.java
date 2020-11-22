package com.demo.rocketmq.simple;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 普通消息：发送同步消息
 *
 * @Author 01399565
 * @create 2020/11/3 16:43
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("sync-message-producer-group");
        // 设置NameServer地址
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        // 启动producer实例
        producer.start();

        for (int i = 0; i < 10; i++) {
            val msg = new Message("simple_msg_topic", "sync", "key-" + i,
                    ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送消息到一个broker
            SendResult result = producer.send(msg);
            // 通过SendResult返回消息是否成功送达
            System.out.printf("%s%n", result);
        }
        // 若不在发送消息，则关闭producer实例
        producer.shutdown();
    }
}
