package com.demo.rocketmq.delaytime;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 延时消息生产者
 *      String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
 *
 * @Author 01399565
 * @create 2020/11/4 11:13
 */
public class DelayTimeProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("delaytime-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();
        // 发送延时消息
        for (int i = 0; i < 5; i++) {
            val msg = new Message("delaytime_msg_topic", ("Delay message" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 设置延时等级为4，即: 30秒
            msg.setDelayTimeLevel(4);
            producer.send(msg);
        }
        producer.shutdown();
    }
}
