package com.open.openmq.client;

import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.producer.DefaultMQProducer;
import com.open.openmq.common.message.Message;

/**
 * @Description TODO
 * @Date 2023/2/11 3:42
 * @Author jack wu
 */
public class Validators {
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        // topic
        Validators.checkTopic(msg.getTopic());
        Validators.isNotAllowedSendTopic(msg.getTopic());

        // body
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }

        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }

        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL,
                    "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
    }
}
