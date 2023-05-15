
package com.open.openmq.client.consumer.listener;


import com.open.openmq.common.message.MessageExt;

import java.util.List;

/**
 * A MessageListenerConcurrently object is used to receive asynchronously delivered messages concurrently
 */
public interface MessageListenerConcurrently extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
     * consumption failure
     *
     * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs, final ConsumeConcurrentlyContext context);
}
