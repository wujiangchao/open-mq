
package com.open.openmq.client.consumer.listener;


import com.open.openmq.common.message.MessageQueue;

/**
 * Consumer Orderly consumption context
 */
public class ConsumeOrderlyContext {
    private final MessageQueue messageQueue;
    private boolean autoCommit = true;
    private long suspendCurrentQueueTimeMillis = -1;

    public ConsumeOrderlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }
}
