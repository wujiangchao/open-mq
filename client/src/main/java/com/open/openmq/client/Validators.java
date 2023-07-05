package com.open.openmq.client;

import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.producer.DefaultMQProducer;
import com.open.openmq.common.UtilAll;
import com.open.openmq.common.message.Message;
import com.open.openmq.common.protocol.ResponseCode;
import com.open.openmq.common.topic.TopicValidator;

/**
 * @Description TODO
 * @Date 2023/2/11 3:42
 * @Author jack wu
 */
public class Validators {

    public static final int CHARACTER_MAX_LENGTH = 255;
    public static final int TOPIC_MAX_LENGTH = 127;

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



    /**
     * Validate group
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }

        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }


        if (TopicValidator.isTopicOrGroupIllegal(group)) {
            throw new MQClientException(String.format(
                    "the specified group[%s] contains illegal characters, allowing only %s", group,
                    "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    public static void checkTopic(String topic) throws MQClientException {
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(
                    String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }

        if (TopicValidator.isTopicOrGroupIllegal(topic)) {
            throw new MQClientException(String.format(
                    "The specified topic[%s] contains illegal characters, allowing only %s", topic,
                    "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    public static void isNotAllowedSendTopic(String topic) throws MQClientException {
        if (TopicValidator.isNotAllowedSendTopic(topic)) {
            throw new MQClientException(
                    String.format("Sending message to topic[%s] is forbidden.", topic), null);
        }
    }

}
