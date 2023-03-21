package com.open.openmq.client.impl.consumer;

import com.open.openmq.common.protocol.heartbeat.ConsumeType;
import com.open.openmq.common.protocol.heartbeat.MessageModel;

/**
 * @Description TODO
 * @Date 2023/3/19 21:33
 * @Author jack wu
 */
public interface MQConsumerInner {
    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();

    void doRebalance();
}
