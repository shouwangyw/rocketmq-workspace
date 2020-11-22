package com.demo.rocketmq.ordered;

import com.demo.rocketmq.common.Constants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 顺序消息消费者
 *
 * @Author 01399565
 * @create 2020/11/4 9:19
 */
public class OrderedConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ordered-message-consumer-group");
        consumer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("ordered_msg_topic", "TagA || TagC || TagD");
        // MessageListenerOrderly：进行消息顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(false);
                for (MessageExt msg : msgs) {
                    System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + new String(msg.getBody()));
                }
                this.consumeTimes.incrementAndGet();
                if ((this.consumeTimes.get() % 2) == 0) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else if ((this.consumeTimes.get() % 3) == 0) {
                    return ConsumeOrderlyStatus.ROLLBACK;
                } else if ((this.consumeTimes.get() % 4) == 0) {
                    return ConsumeOrderlyStatus.COMMIT;
                } else if ((this.consumeTimes.get() % 5) == 0) {
                    context.setSuspendCurrentQueueTimeMillis(3000);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer started");
    }
}
/**
 *         ---------------------
 * queue0: | 0 | 2 | 4 | 6 | 8 |
 *         | A | C | E | B | D |
 *         ---------------------
 *           √   √           √          ==>>  0, 2, 8
 *
 *         ---------------------
 * queue1: | 1 | 3 | 5 | 7 | 9 |
 *         | B | D | A | C | E |
 *         ---------------------
 *               √   √   √              ==>> 3, 5, 7
 */