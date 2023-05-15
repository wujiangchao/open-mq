
package com.open.openmq.client.consumer.listener;


import com.open.openmq.common.message.MessageExt;

import java.util.List;

/**
 * A MessageListenerOrderly object is used to receive messages orderly. One queue by one thread
 */
public interface MessageListenerOrderly extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
     * if consumption failure
     *
     * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,final ConsumeOrderlyContext context);
}
