package com.demo.rocketmq.broadcasting;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 广播消息生产者
 *
 * @Author 01399565
 * @create 2020/11/4 10:54
 */
public class BroadcastingProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("broadcasting-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC"};
        for (int i = 0; i < 10; i++) {
            val msg = new Message("broadcasting_msg_topic", tags[i % tags.length], "key-" + i,
                    ("Broadcasting message" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
