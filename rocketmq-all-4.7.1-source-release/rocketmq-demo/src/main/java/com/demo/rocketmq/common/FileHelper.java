package com.demo.rocketmq.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author 01399565
 * @create 2020/11/4 19:32
 */
public class FileHelper {
    private FileHelper() {
    }

    public static ByteBuffer read(String path) throws IOException {
        File file = new File(path);
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] bytes = new byte[1024 * 1024 * 1024];
            fis.read(bytes);
            return ByteBuffer.wrap(bytes);
        }
    }
}
