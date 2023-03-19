package com.open.openmq.client.consumer;

import com.open.openmq.client.ClientConfig;
import com.open.openmq.client.exception.MQBrokerException;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * @Description TODO
 * @Date 2023/3/17 17:21
 * @Author jack wu
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

    /**
     * Internal implementation. Most of the functions herein are delegated to it.
     */
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return 0;
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return null;
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return null;
    }

    @Override
    public void start() throws MQClientException {
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPushConsumerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {

    }

    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {

    }

    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {

    }

    @Override
    public void subscribe(String topic, MessageSelector selector) throws MQClientException {

    }

    @Override
    public void unsubscribe(String topic) {

    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {

    }

    @Override
    public void suspend() {

    }

    @Override
    public void resume() {

    }
}
