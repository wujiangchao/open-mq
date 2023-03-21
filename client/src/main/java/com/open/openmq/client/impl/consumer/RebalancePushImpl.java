package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.impl.factory.MQClientInstance;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.heartbeat.MessageModel;
import com.open.openmq.common.protocol.heartbeat.SubscriptionData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description TODO
 * @Date 2023/3/20 16:49
 * @Author jack wu
 */
public class RebalancePushImpl extends RebalanceImpl{

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
                             AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                             MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

}
