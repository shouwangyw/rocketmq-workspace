package com.demo.rocketmq.ordered;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 顺序消息生产者
 *
 * @Author 01399565
 * @create 2020/11/3 18:47
 */
public class OrderedProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ordered-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            int orderId = i % 2;
            val msg = new Message("ordered_msg_topic", tags[i % tags.length], "key-" + i,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // MessageQueueSelector
            // 选择指定队列进行消息发送
            SendResult result = producer.send(msg, (mqs, msg1, arg) -> {
                Integer id = (Integer) arg;
                return mqs.get(id % mqs.size());
            }, orderId);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
/**
 *         ---------------------
 * queue0: | 0 | 2 | 4 | 6 | 8 |
 *         | A | C | E | B | D |
 *         ---------------------
 *
 *         ---------------------
 * queue1: | 1 | 3 | 5 | 7 | 9 |
 *         | B | D | A | C | E |
 *         ---------------------
 */