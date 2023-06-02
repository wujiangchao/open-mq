package com.open.openmq.common.message;

import java.io.Serializable;

/**
 * @Description 消息队列实体
 * @Date 2023/2/14 14:37
 * @Author jack wu
 */
public class MessageQueue implements Comparable<MessageQueue>, Serializable {
    private static final long serialVersionUID = 6191200464116433425L;
    private String topic;
    private String brokerName;
    private int queueId;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public MessageQueue(MessageQueue other) {
        this.topic = other.topic;
        this.brokerName = other.brokerName;
        this.queueId = other.queueId;
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    @Override
    public int compareTo(MessageQueue o) {
        return 0;
    }
}
