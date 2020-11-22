package com.demo.rocketmq.commitlog;

import com.demo.rocketmq.common.Constants;
import com.demo.rocketmq.common.FileHelper;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author 01399565
 * @create 2020/11/4 18:39
 */
public class CommitLogRead {
    public static void main(String[] args) throws Exception {
        String filePath = Constants.STORE_PATH + "/commitlog/00000000000000000000";
        ByteBuffer byteBuffer = FileHelper.read(filePath);
        List<MessageExt> messageExts = new ArrayList<>();
        while (true) {
            MessageExt messageExt = MessageDecoder.decode(byteBuffer);
            if (messageExt == null || messageExts.size() >= 10) {
                break;
            }
            messageExts.add(messageExt);
        }
        System.out.println(messageExts.size());
        for (MessageExt msg : messageExts) {
            if (msg.getBody() == null) {
                continue;
            }
            System.out.println("主题：" + msg.getTopic() + "，消息：" + new String(msg.getBody()) +
                    "，队列ID：" + msg.getQueueId() + "，存储地址：" + msg.getStoreHost());
        }
        System.out.println("==End==");
    }
}
