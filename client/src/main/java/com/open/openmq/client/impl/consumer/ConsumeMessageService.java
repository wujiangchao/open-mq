
package com.open.openmq.client.impl.consumer;


import com.open.openmq.common.message.MessageQueue;

import java.util.List;

/**
 * 拉取消息的核心处理类。
 * 一共有两种实现，并发消费和顺序消费。
 */
public interface ConsumeMessageService {
    void start();

    void shutdown(long awaitTerminateMillis);

    void updateCorePoolSize(int corePoolSize);

    void incCorePoolSize();

    void decCorePoolSize();

    int getCorePoolSize();

    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume);

    void submitPopConsumeRequest(
            final List<MessageExt> msgs,
            final PopProcessQueue processQueue,
            final MessageQueue messageQueue);
}
