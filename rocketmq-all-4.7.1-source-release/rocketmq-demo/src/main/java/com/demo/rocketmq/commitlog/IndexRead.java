package com.demo.rocketmq.commitlog;

import com.demo.rocketmq.common.Constants;
import com.demo.rocketmq.common.FileHelper;

import java.nio.ByteBuffer;

/**
 * @Author 01399565
 * @create 2020/11/4 20:29
 */
public class IndexRead {
    public static void main(String[] args) throws Exception {
        // index索引文件的路径
        String path = Constants.STORE_PATH + "/index/20201104152325108";
        ByteBuffer buffer = FileHelper.read(path);
        // 该索引文件中包含消息的最小存储时间
        long beginTimestamp = buffer.getLong();
        // 该索引文件中包含消息的最大存储时间
        long endTimestamp = buffer.getLong();
        // 该索引文件中包含消息的最大物理偏移量(commitlog文件偏移量)
        long beginPhyOffset = buffer.getLong();
        // 该索引文件中包含消息的最大物理偏移量(commitlog文件偏移量)
        long endPhyOffset = buffer.getLong();
        // hashslot个数
        int hashSlotCount = buffer.getInt();
        // Index条目列表当前已使用的个数
        int indexCount = buffer.getInt();

        // 500万个hash槽，每个槽占4个字节，存储的是index索引
        for (int i = 0; i < 5000000; i++) {
            buffer.getInt();
        }
        // 2000万个index条目
        for (int j = 0; j < 20000000; j++) {
            // 消息key的hashcode
            int hashcode = buffer.getInt();
            // 消息对应的偏移量
            long offset = buffer.getLong();
            // 消息存储时间和第一条消息的差值
            int timedif = buffer.getInt();
            // 该条目的上一条记录的index索引
            int pre_no = buffer.getInt();
        }
        System.out.println("position:" + buffer.position() + ", capacity:" + buffer.capacity());
        System.out.println(buffer.position() == buffer.capacity());
    }
}
