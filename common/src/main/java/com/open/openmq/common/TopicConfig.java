package com.open.openmq.common;

import com.alibaba.fastjson.TypeReference;

import java.util.Map;

/**
 * @Description TODO
 * @Date 2023/3/24 13:42
 * @Author jack wu
 */
public class TopicConfig {
    private static final String SEPARATOR = " ";
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;
    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };
    private String topicName;
    private int readQueueNums = defaultReadQueueNums;
    private int writeQueueNums = defaultWriteQueueNums;
    private int topicSysFlag = 0;
    private boolean order = false;

    public static int getDefaultReadQueueNums() {
        return defaultReadQueueNums;
    }

    public static void setDefaultReadQueueNums(int defaultReadQueueNums) {
        TopicConfig.defaultReadQueueNums = defaultReadQueueNums;
    }

    public static int getDefaultWriteQueueNums() {
        return defaultWriteQueueNums;
    }

    public static void setDefaultWriteQueueNums(int defaultWriteQueueNums) {
        TopicConfig.defaultWriteQueueNums = defaultWriteQueueNums;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }
}
