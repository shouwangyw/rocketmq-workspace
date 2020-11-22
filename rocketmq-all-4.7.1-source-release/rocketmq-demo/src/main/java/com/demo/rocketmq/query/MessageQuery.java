package com.demo.rocketmq.query;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 消息查询
 *
 * @Author 01399565
 * @create 2020/11/4 20:40
 */
public class MessageQuery {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("query-message-producer-message");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();
        // 根据UniqueKey查询
        String offsetMsgId = "C0A8696400002A9F0000000000005727";
        MessageExt msg = producer.viewMessage("simple_msg_topic", offsetMsgId);
        MessageClientExt messageClientExt = (MessageClientExt) msg;
        // 打印结果：这里仅输出Unique Key与offsetMsgId
        System.out.println("Unique Key: " + messageClientExt.getMsgId() +
                "\noffsetMsgId: " + messageClientExt.getOffsetMsgId());

        producer.shutdown();
    }
}
