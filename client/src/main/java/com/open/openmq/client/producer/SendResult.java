package com.open.openmq.client.producer;

import com.open.openmq.client.impl.producer.SendStatus;
import com.open.openmq.common.message.MessageQueue;

/**
 * @Description TODO
 * @Date 2023/2/14 14:41
 * @Author jack wu
 */
public class SendResult {
    private SendStatus sendStatus;
    private String msgId;
    private MessageQueue messageQueue;
    private long queueOffset;
    private String transactionId;
    private String offsetMsgId;
    private String regionId;

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(boolean traceOn) {
        this.traceOn = traceOn;
    }

    private boolean traceOn = true;
    private byte[] rawRespBody;

    public SendResult() {
    }

    public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue,
                      long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.offsetMsgId = offsetMsgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }


    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }
}
