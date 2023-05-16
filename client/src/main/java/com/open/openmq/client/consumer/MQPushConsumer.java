package com.open.openmq.client.consumer;

import com.open.openmq.client.MessageSelector;
import com.open.openmq.client.consumer.listener.MessageListenerConcurrently;
import com.open.openmq.client.consumer.listener.MessageListenerOrderly;
import com.open.openmq.client.exception.MQClientException;

/**
 * @Description TODO
 * @Date 2023/3/19 10:46
 * @Author jack wu
 */
public interface MQPushConsumer extends MQConsumer{
    /**
     * Start the consumer
     */
    void start() throws MQClientException;

    /**
     * Shutdown the consumer
     */
    void shutdown();

    /**
     * 注册并发消息事件监听器
     * @param messageListener
     */
    void registerMessageListener(final MessageListenerConcurrently messageListener);

    /**
     * 注册顺序消息事件监听器
     * @param messageListener
     */
    void registerMessageListener(final MessageListenerOrderly messageListener);

    /**
     * Subscribe some topic
     *
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
     * null or * expression,meaning subscribe
     * all
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * Subscribe some topic with selector.
     * <p>
     * This interface also has the ability of {@link #subscribe(String, String)},
     * and, support other message selection, such as {@link com.open.openmq.common.filter.ExpressionType#SQL92}.
     * </p>
     * <p/>
     * <p>
     * Choose Tag: {@link MessageSelector#byTag(java.lang.String)}
     * </p>
     * <p/>
     * <p>
     * Choose SQL92: {@link MessageSelector#bySql(java.lang.String)}
     * </p>
     *
     * @param selector message selector({@link MessageSelector}), can be null.
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * Unsubscribe consumption some topic
     *
     * @param topic message topic
     */
    void unsubscribe(final String topic);

    /**
     * Update the consumer thread pool size Dynamically
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * Suspend the consumption
     */
    void suspend();

    /**
     * Resume the consumption
     */
    void resume();

}
