package com.demo.rocketmq.simple;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 普通消息：发送异步消息
 *
 * @Author 01399565
 * @create 2020/11/3 16:43
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("async-message-producer-group");
        // 设置NameServer地址
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        // 启动producer实例
        producer.start();
        // 设置同步发送消息重试次数为0，即失败后不重试
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int msgCount = 10;
        final val latch = new CountDownLatch(msgCount);
        for (int i = 0; i < msgCount; i++) {
            try {
                final int index = i;
                val msg = new Message("simple_msg_topic", "async", "key" + i,
                        ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 发送异步消息，SendCallback接收异步返回结果的回调
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        latch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }
                    @Override
                    public void onException(Throwable e) {
                        latch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        latch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }
}
