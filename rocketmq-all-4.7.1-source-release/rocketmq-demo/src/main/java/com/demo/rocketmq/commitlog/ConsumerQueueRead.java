package com.demo.rocketmq.commitlog;

import com.demo.rocketmq.common.Constants;
import com.demo.rocketmq.common.FileHelper;

import java.nio.ByteBuffer;

/**
 * @Author 01399565
 * @create 2020/11/4 20:25
 */
public class ConsumerQueueRead {
    public static void main(String[] args) throws Exception {
        String path = Constants.STORE_PATH + "/commitlog/00000000000000000000";
        ByteBuffer buffer = FileHelper.read(path);
        while (true) {
            long offset = buffer.getLong();
            int size = buffer.getInt();
            long code = buffer.getLong();
            if (size == 0) {
                break;
            }
            System.out.println("消息长度：" + size + "，消息偏移量：" + offset);
        }
    }
}
