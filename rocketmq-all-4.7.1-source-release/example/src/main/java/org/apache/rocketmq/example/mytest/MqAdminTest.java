package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author yangwei
 * @date 2020-10-27 21:47
 */
public class MqAdminTest {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group-1");
        producer.setNamesrvAddr("192.168.254.130:9876");
        producer.start();

        // 根据UniqueKey查询
        String uniqueKey = "C0A80064282518B4AAC28AAB390D0000";
        MessageExt msg = producer.viewMessage("TopicA", uniqueKey);
        // 打印结果:这里仅输出Unique Key与offsetMsgId
        MessageClientExt msgExt = (MessageClientExt) msg;
        System.out.println("Unique Key:" + msgExt.getMsgId()
                + "\noffsetMsgId:" + msgExt.getOffsetMsgId());
    }
}
