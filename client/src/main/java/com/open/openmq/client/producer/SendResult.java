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
    private boolean traceOn = true;
    private byte[] rawRespBody;

    public SendResult() {
    }
}
