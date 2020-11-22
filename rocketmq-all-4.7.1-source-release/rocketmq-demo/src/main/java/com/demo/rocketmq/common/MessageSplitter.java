package com.demo.rocketmq.common;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author 01399565
 * @create 2020/11/4 14:22
 */
public class MessageSplitter implements Iterator<List<Message>> {
    private static final long DEFAULT_SIZE_LIMIT = 1000 * 1000L;
    private final List<Message> messages;
    private final long sizeLimit;
    private int currentIndex;

    public MessageSplitter(List<Message> messages) {
        this(messages, DEFAULT_SIZE_LIMIT);
    }

    public MessageSplitter(List<Message> list, long sizeLimit) {
        this.messages = list;
        this.sizeLimit = sizeLimit;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currentIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            // for log overhead
            tmpSize += 20;
            if (tmpSize > sizeLimit) {
                // it is unexpected that single message exceeds the SIZE_LIMIT
                // here just let it go, otherwise it will block the splitting process
                if (nextIndex - currentIndex == 0) {
                    // if the next sublist has no element, add this one and then break, otherwise just break
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > sizeLimit) {
                break;
            } else {
                totalSize += tmpSize;
            }
        }

        List<Message> subList = messages.subList(currentIndex, nextIndex);
        currentIndex = nextIndex;
        return subList;
    }
}
