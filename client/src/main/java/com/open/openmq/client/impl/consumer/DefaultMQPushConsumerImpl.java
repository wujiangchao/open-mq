package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.MessageSelector;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.common.ServiceState;
import com.open.openmq.common.protocol.heartbeat.SubscriptionData;

/**
 * @Description TODO
 * @Date 2023/3/19 21:32
 * @Author jack wu
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);


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


    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                //将消费者服务预设为 启动失败
                this.serviceState = ServiceState.START_FAILED;

                //校验一堆配置  消费组配置规则、消费并发线程数等
                this.checkConfig();

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
                    this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    this.consumeMessageService =
                            new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService =
                            new ConsumeMessagePopConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                this.consumeMessageService.start();
                // POPTODO
                this.consumeMessagePopService.start();

                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        this.mQClientFactory.checkClientInBroker();
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        this.mQClientFactory.rebalanceImmediately();
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
}
