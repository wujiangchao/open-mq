package com.open.openmq.client.producer;

import com.open.openmq.client.ClientConfig;
import com.open.openmq.client.exception.MQClientException;

/**
 * @Description 默认的生产者实现
 * @Date 2023/1/30 23:48
 * @Author jack wu
 */
public class DefaultMQProducer extends ClientConfig implements Producer{

    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;


    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved. </p>
     *
     * For non-transactional messages, it does not matter as long as it's unique per process. </p>
     *
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">core concepts</a> for more discussion.
     */
    private String producerGroup;

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    /**
     * Constructor specifying namespace, producer group and RPC hook.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

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
    public void start() throws MQClientException {

    }

    @Override
    public void shutdown() {

    }
}
