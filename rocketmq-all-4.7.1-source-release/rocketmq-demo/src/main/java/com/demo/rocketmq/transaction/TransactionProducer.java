package com.demo.rocketmq.transaction;

import com.demo.rocketmq.common.Constants;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 事务消息生产者
 *
 * @Author 01399565
 * @create 2020/11/4 15:30
 */
public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        // 创建事务消息生产者
        TransactionMQProducer producer = new TransactionMQProducer("transaction-message-producer-group");
        producer.setNamesrvAddr(Constants.NAMESRV_ADDR);
        // 添加事务监听器
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 在该方法内执行本地事务
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                String tags = msg.getTags();
                System.out.println("Execute Local Transaction: " + tags);
                if (StringUtils.equals("TagA", tags)) {
                    // 正常提交
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("TagB", tags)) {
                    // 回滚
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else if (StringUtils.equals("TagC", tags)) {
                    // 未知，会进行本地回查
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            /**
             * 在该方法内进行消息回查
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                // 本地进行消息回查，正常提交
                System.out.println("Check Local Transaction: " + msg.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        String[] tags = {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 10; i++) {
            val msg = new Message("transaction_msg_topic", tags[i % tags.length],
                    ("transaction message" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送事务消息
            TransactionSendResult result = producer.sendMessageInTransaction(msg, null);
            SendStatus sendStatus = result.getSendStatus();
            System.out.println("发送结果：" + sendStatus);
            TimeUnit.SECONDS.sleep(1);
        }
//        producer.shutdown();
    }
}
