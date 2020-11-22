package com.demo.rocketmq.batch;

import com.demo.rocketmq.common.Constants;
import com.demo.rocketmq.common.MessageSplitter;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息生产者
 *
 * @Author 01399565
 * @create 2020/11/4 14:19
 */
public class BatchProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("batch-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();

        String topic = "batch_msg_topic";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "orderId-0001", "batch message 0001".getBytes()));
        messages.add(new Message(topic, "TagA", "orderId-0002", "batch message 0002".getBytes()));
        messages.add(new Message(topic, "TagA", "orderId-0003", "batch message 0003".getBytes()));

        val splitter = new MessageSplitter(messages);
        while (splitter.hasNext()) {
            try {
                List<Message> item = splitter.next();
                SendResult result = producer.send(item);
                System.out.println(result);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        producer.shutdown();
    }
}
