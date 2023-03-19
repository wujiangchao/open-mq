package com.open.openmq.client.impl.consumer;

/**
 * @Description TODO
 * @Date 2023/3/19 21:33
 * @Author jack wu
 */
public interface MQConsumerInner {
    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();
}
