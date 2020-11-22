package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangwei
 * @date 2020-10-27 21:47
 */
public class ProducerTest {
//    public static void main(String[] args) throws Exception {
//        DefaultMQProducer producer = new DefaultMQProducer("producer-group-1");
//        producer.setNamesrvAddr("192.168.254.130:9876");
////        producer.setVipChannelEnabled(false);
//        producer.start();
//
//        Message msg = new Message();
//        msg.setTopic("TopicA");
//        msg.setKeys("Key1");
//        msg.setBody("Hello world".getBytes());
//
//        try {
//            SendResult result = producer.send(msg);
//            System.out.println(result);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        producer.shutdown();
//    }
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group-1");
        producer.setNamesrvAddr("192.168.254.130:9876");
        producer.start();

        // 构建批量消息
        List<Message> msgs = new ArrayList<>();
        msgs.add(new Message("TopicB", "message5".getBytes()));
        msgs.add(new Message("TopicB", "message6".getBytes()));

        SendResult result = producer.send(msgs);
        System.out.println(result);
        producer.shutdown();
    }
}
