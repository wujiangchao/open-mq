package com.open.openmq.client.consumer;

import com.open.openmq.client.MQAdmin;
import com.open.openmq.client.exception.MQBrokerException;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.common.message.MessageExt;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * @Description TODO
 * @Date 2023/3/19 10:43
 * @Author jack wu
 */
public interface MQConsumer extends MQAdmin {
    /**
     * If consuming failure,message will be send back to the broker,and delay consuming some time
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * Fetch message queues from consumer cache according to the topic
     *
     * @param topic message topic
     * @return queue set
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
