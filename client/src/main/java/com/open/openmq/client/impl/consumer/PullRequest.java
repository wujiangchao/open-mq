package com.open.openmq.client.impl.consumer;

import com.open.openmq.common.message.MessageQueue;

/**
 * @Description TODO
 * @Date 2023/3/21 14:42
 * @Author jack wu
 */
public class PullRequest implements MessageRequest{
    /**
     * 消费者组
     */
    private String consumerGroup;
    /**
     * 待拉取消息队列
     */
    private MessageQueue messageQueue;
    /**
     * 消息处理队列
     */
    private ProcessQueue processQueue;
    /**
     * 待拉取的MessageQueue偏移量
     */
    private long nextOffset;
    /**
     * 是否被锁定
     */
    private boolean previouslyLocked = false;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public ProcessQueue getProcessQueue() {
        return processQueue;
    }

    public void setProcessQueue(ProcessQueue processQueue) {
        this.processQueue = processQueue;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public boolean isPreviouslyLocked() {
        return previouslyLocked;
    }

    public void setPreviouslyLocked(boolean previouslyLocked) {
        this.previouslyLocked = previouslyLocked;
    }
}
