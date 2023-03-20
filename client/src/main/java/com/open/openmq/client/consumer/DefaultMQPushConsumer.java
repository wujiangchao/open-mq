package com.open.openmq.client.consumer;

import com.open.openmq.client.ClientConfig;
import com.open.openmq.client.MessageSelector;
import com.open.openmq.client.exception.MQBrokerException;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.Map;
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

    private String consumerGroup;

    /**
     * Message model defines the way how messages are delivered to each consumer clients.
     * </p>
     *
     * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
     * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
     * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
     * separately.
     * </p>
     *
     * This field defaults to clustering.
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Consuming point on consumer booting.
     * </p>
     *
     * There are three consuming points:
     * <ul>
     * <li>
     * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
     * If it were a newly booting up consumer client, according aging of the consumer group, there are two
     * cases:
     * <ol>
     * <li>
     * if the consumer group is created so recently that the earliest message being subscribed has yet
     * expired, which means the consumer group represents a lately launched business, consuming will
     * start from the very beginning;
     * </li>
     * <li>
     * if the earliest message being subscribed has expired, consuming will start from the latest
     * messages, meaning messages born prior to the booting timestamp would be ignored.
     * </li>
     * </ol>
     * </li>
     * <li>
     * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
     * </li>
     * <li>
     * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
     * messages born prior to {@link #consumeTimestamp} will be ignored
     * </li>
     * </ul>
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Backtracking consumption time with second precision. Time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago.
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();

    /**
     * Message listener
     */
    private MessageListener messageListener;

    /**
     * Offset Storage
     * 消费进度存储器
     */
    private OffsetStore offsetStore;

    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 20;

    /**
     * 消费队列中消息最大跨度：最大消息的偏移量 - 最小消息的偏移量
     * 跨度默认2000，超出时延迟50ms再拉取消息
     */
    private int consumeConcurrentlyMaxSpan = 2000;
    // 消息拉取间隔时间
    private long pullInterval = 0;
    // 并发消费时一次消费消息条数
    private int consumeMessageBatchMaxSize = 1;
    // 每次拉取消息所拉取的消息条数，默认32
    private int pullBatchSize = 32;
    // 每次拉取消息时是否更新订阅信息，默认false
    private boolean postSubscriptionWhenPull = false;
    // 最大消费重试次数
    private int maxReconsumeTimes = -1;
    // 消息延迟到消费线程的时间，默认1s
    private long suspendCurrentQueueTimeMillis = 1000;
    // 消费超时时间，默认15min
    private long consumeTimeout = 15;

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
