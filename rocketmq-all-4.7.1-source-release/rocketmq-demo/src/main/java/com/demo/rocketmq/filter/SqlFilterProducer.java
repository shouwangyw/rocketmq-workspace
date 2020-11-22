package com.demo.rocketmq.filter;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 过滤消息生产者：通过Sql进行过滤
 *
 * @Author 01399565
 * @create 2020/11/4 14:53
 */
public class SqlFilterProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("filter-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();

        String[] tags = {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 10; i++) {
            String tag = tags[i % tags.length];
            val msg = new Message("filter_msg_topic", tag,
                    ("sql filter message " + i + "，消息Tag：" + tag + "，属性age：" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 设置用户属性
            msg.putUserProperty("age", String.valueOf(i));
            SendResult result = producer.send(msg);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
