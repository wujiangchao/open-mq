package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.MessageSelector;
import com.open.openmq.client.consumer.DefaultMQPushConsumer;
import com.open.openmq.client.consumer.PullCallback;
import com.open.openmq.client.consumer.PullResult;
import com.open.openmq.client.consumer.listener.MessageListener;
import com.open.openmq.client.consumer.listener.MessageListenerConcurrently;
import com.open.openmq.client.consumer.listener.MessageListenerOrderly;
import com.open.openmq.client.consumer.store.LocalFileOffsetStore;
import com.open.openmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.hook.FilterMessageHook;
import com.open.openmq.client.impl.CommunicationMode;
import com.open.openmq.client.impl.MQClientManager;
import com.open.openmq.client.impl.factory.MQClientInstance;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.client.consumer.store.OffsetStore;
import com.open.openmq.client.consumer.store.ReadOffsetType;
import com.open.openmq.client.stat.ConsumerStatsManager;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.ServiceState;
import com.open.openmq.common.consumer.ConsumeFromWhere;
import com.open.openmq.common.filter.FilterAPI;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.ResponseCode;
import com.open.openmq.common.protocol.heartbeat.ConsumeType;
import com.open.openmq.common.protocol.heartbeat.MessageModel;
import com.open.openmq.common.protocol.heartbeat.SubscriptionData;
import com.open.openmq.common.sysflag.PullSysFlag;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.remoting.RPCHook;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @Description TODO
 * @Date 2023/3/19 21:32
 * @Author jack wu
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();

    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 3000;

    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;

    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;

    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    private MQClientInstance mQClientFactory;

    private PullAPIWrapper pullAPIWrapper;

    private OffsetStore offsetStore;

    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);

    private final DefaultMQPushConsumer defaultMQPushConsumer;

    private ConsumeMessageService consumeMessageService;

    private ConsumeMessageService consumeMessagePopService;

    private final RPCHook rpcHook;

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private MessageListener messageListenerInner;

    private volatile boolean pause = false;

    private boolean consumeOrderly = false;

    /**
     * 数量流控次数
     */
    private long queueFlowControlTimes = 0;
    /**
     * MaxSpan 流控次数
     */
    private long queueMaxSpanFlowControlTimes = 0;


    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        // this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                //将消费者服务预设为 启动失败
                this.serviceState = ServiceState.START_FAILED;

                //校验一堆配置  消费组配置规则、消费并发线程数等
                //this.checkConfig();

                //copy 订阅关系，订阅重试主题 %retry%+消费组名称
                this.copySubscription();

                //如果消费模式是集群模式，将消费者示例的name 修改为PID
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }

                //初始化mq客户端连接工厂
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                //消息重新负载消费
                //指定消费组
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                //指定消息传播方式
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                //指定队列分配算法
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                //指定MQclient工厂
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                if (this.pullAPIWrapper == null) {
                    this.pullAPIWrapper = new PullAPIWrapper(
                            mQClientFactory,
                            this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                }
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                this.offsetStore.load();

                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService =
                            new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                   // this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    this.consumeMessageService =
                            new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
//                    //POPTODO reuse Executor ?
//                    this.consumeMessagePopService =
//                            new ConsumeMessagePopConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                this.consumeMessageService.start();
                // POPTODO
                //this.consumeMessagePopService.start();

                //注册到消费者实例到客户端
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." ,
                            null);
                }
                //启动客户端
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState,
                        null);
            default:
                break;
        }

        //更新订阅信息
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        //检测broker状态
        this.mQClientFactory.checkClientInBroker();
        //发送心跳包
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        //重新负载
        this.mQClientFactory.rebalanceImmediately();
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            //订阅Topic下所有信息
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            // 构建订阅信息
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());
            // 订阅添加到负载均衡中，后创建拉取消息任务
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                // 加锁定时发送到所有Broker（类过滤源代码发送到过滤服务器上）
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        //如果处理队列当前状态被丢弃，结束本次消息拉取
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }
        //如果没有被丢弃，更新ProcessQueue的lastPullTimestamp为当前时间戳
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            //如果消费者状态异常，则将拉取任务延迟1s再次放入到PullMessageService的拉取任务队列中
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }

        //如果当前消费者被挂起，则将拉取任务延迟1s再次放入到PullMessageService的拉取任务队列中，结束本次消息拉取
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        //进行消息拉取流控。从消息消费数量与消费间隔两个维度进行控制。
        long cachedMessageCount = processQueue.getMsgCount().get();
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        // 未消息处理总数，如果ProcessQueue当前处理的消息条数超过了1000将触发流控，放弃本次拉取任务，
        // 并且该队列的下一次拉取任务将在50毫秒后才加入到拉取任务队列中，每触发1000次流控后输出提示语。
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //未处理消息大小，如果ProcessQueue当前处理的消息大小超过100MB，放弃本次拉取任务，并且该队列的下一次拉取任务
        //将在50毫秒后才加入到拉取任务队列中，每触发1000次流控后输出提示语
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }
        //ProcessQueue中队列最大偏移量与最小偏离量的间距
        //非顺序消息，默认情况不能超过2000，否则触发流控，放弃本次拉取任务，并且该队列的下一次拉取任务将在50毫秒后才加入到拉取任务队列中
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                            "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                            pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else {
            //如果ProcessQueue是锁定状态，获取服务端的消费偏移量offset
            //pullRequest.setNextOffset(offset)
            if (processQueue.isLocked()) {
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                        if (offset < 0) {
                            throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Unexpected offset " + offset);
                        }
                    } catch (Exception e) {
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                            pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                                pullRequest, offset);
                    }

                    pullRequest.setPreviouslyLocked(true);
                    pullRequest.setNextOffset(offset);
                }
            } else {
                //如果ProcessQueue未锁定状态，放弃本次拉取任务，并且该队列的下一次拉取任务将在50毫秒后才加入到拉取任务队列中
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }
        //拉取该主题订阅信息，如果为空，结束本次消息拉取，关于该队列的下一次拉取任务延迟3s
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                            subscriptionData);

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            long prevRequestOffset = pullRequest.getNextOffset();
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                        pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                //首先将拉取到的消息存入ProcessQueue，然后将拉取到的消息提交到ConsumeMessageService中供消费者消费，
                                //该方法是一个异步方法，也就是PullCallBack将消息提交到ConsumeMessageService中就会立即返回，至于这些消息如何消费，
                                // PullCallBack不关注。
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                        pullResult.getMsgFoundList(),
                                        processQueue,
                                        pullRequest.getMessageQueue(),
                                        dispatchToConsume);

                                //然后根据pullInterval参数，如果pullInterval>0，则等待pullInterval毫秒后将PullRequest对象放入到
                                //PullMessageService的pullRequestQueue中，该消息队列的下次拉取即将被激活，达到持续消息拉取，实现准实时拉取消息的效果。
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                            DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                    || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                        pullResult.getNextBeginOffset(),
                                        firstMsgOffset,
                                        prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}",
                                    pullRequest.toString(), pullResult.toString());
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            pullRequest.getProcessQueue().setDropped(true);
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                                pullRequest.getNextOffset(), false);

                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());

                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        int sysFlag = PullSysFlag.buildSysFlag(
                commitOffsetEnable, // commitOffset
                true, // suspend
                subExpression != null, // subscription
                classFilter // class filter
        );
        //调用PullAPIWrapper.pullKernelImpl方法后与服务端交互
        try {
            this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(), // 从哪个消息消费队列拉取消息
                    subExpression, // 消息过滤表达式
                    subscriptionData.getExpressionType(), // 消息表达式类型，分为TAG、SQL92。
                    subscriptionData.getSubVersion(),
                    pullRequest.getNextOffset(), // 消息拉取偏移量
                    this.defaultMQPushConsumer.getPullBatchSize(), // 本次拉取最大消息条数，默认32条
                    sysFlag,
                    commitOffsetValue,
                    BROKER_SUSPEND_MAX_TIME_MILLIS,
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                    CommunicationMode.ASYNC, // 消息拉取模式，默认为异步拉取
                    pullCallback // 从Broker拉取到消息后的回调方法。
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            // 异常后，放弃本次拉取任务，并且该队列的下一次拉取任务将在50毫秒后才加入到拉取任务队列中
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }


    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    private void copySubscription() throws MQClientException {
        try {
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }



    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }


    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    public void setMessageListenerInner(MessageListener messageListenerInner) {
        this.messageListenerInner = messageListenerInner;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }


    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                    + this.serviceState,
                    null);
        }
    }

    void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    @Override
    public String groupName() {
        return null;
    }

    @Override
    public MessageModel messageModel() {
        return null;
    }

    @Override
    public ConsumeType consumeType() {
        return null;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return null;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return null;
    }

    @Override
    public void doRebalance() {
        if (!this.pause) {
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {

    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {

    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        return false;
    }
}
