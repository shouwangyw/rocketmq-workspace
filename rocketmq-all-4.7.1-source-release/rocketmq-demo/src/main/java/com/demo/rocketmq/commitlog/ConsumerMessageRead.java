package com.demo.rocketmq.commitlog;

import com.demo.rocketmq.common.Constants;
import com.demo.rocketmq.common.FileHelper;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @Author 01399565
 * @create 2020/11/4 19:31
 */
public class ConsumerMessageRead {
    public static void main(String[] args) throws Exception {
        String consumerPath = Constants.STORE_PATH + "/consumequeue";
        String commitLogPath = Constants.STORE_PATH + "/commitlog/00000000000000000000";
        ByteBuffer commitLogBuffer = FileHelper.read(commitLogPath);

        File file = new File(consumerPath);
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                File[] listFiles = f.listFiles();
                for (File queuePath : listFiles) {
                    // 读取consumerqueue文件内容
                    ByteBuffer byteBuffer = FileHelper.read(queuePath + "/00000000000000000000");
                    while (true) {
                        // 读取消息偏移量和消息长度
                        long offset = (int) byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        long code = byteBuffer.getLong();
                        System.out.println("offset = " + offset + ", size = " + size + ", code = " + code);
                        if (size == 0) {
                            break;
                        }
                        // 根据偏移量和消息长度，在commitlog文件中读取消息内容
                        MessageExt message = getMessageByOffset(commitLogBuffer, offset, size);
                        if (message != null) {
                            System.out.println("消息主题：" + message.getTopic() + "，MessageQueue：" + message.getQueueId() +
                                    "，消息体：" + new String(message.getBody()));
                        }
                    }
                }
            }
        }
    }

    private static MessageExt getMessageByOffset(ByteBuffer commitLog, long offset, int size) throws Exception {
        ByteBuffer slice = commitLog.slice();
        slice.position((int) offset);
        slice.limit((int) (offset + size));
        return MessageDecoder.decode(slice);
    }
}
