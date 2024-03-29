package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.consumer.DefaultMQPushConsumer;
import com.open.openmq.client.consumer.listener.MessageListenerOrderly;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.common.ThreadFactoryImpl;
import com.open.openmq.common.message.MessageExt;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.open.openmq.common.protocol.heartbeat.MessageModel;
import com.open.openmq.logging.InternalLogger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description ConsumeMessageOrderlyService是一个服务类，用于实现有序消息消费。有序消息消费是一种特殊的消息消费模式
 * 它保证同一个消息只被一个消费者处理，以确保消息处理的顺序性。
 * 具体来说，ConsumeMessageOrderlyService实现了以下功能：
 * 1、实现了MessageListenerOrderly接口，指定了消息消费的逻辑。
 * 2、维护了一个消费者队列，每个消费者都对应一个队列中的位置。
 * 3、当有新的消息到达时，ConsumeMessageOrderlyService会根据负载均衡策略将消息分配给队列中的某个消费者。
 * 消费者会按照消息的顺序依次处理消息，保证消息处理的顺序性。
 * @Date 2023/3/21 9:33
 * @Author jack wu
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
            Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        String consumeThreadPrefix = null;
        if (consumerGroup.length() > 100) {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup.substring(0, 100)).append("_").toString();
        } else {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
        }
        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl(consumeThreadPrefix));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    @Override
    public void start() {
        //如果是集群模式
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            //启动一个定时任务，启动后1s执行，后续每20s执行一次
            //尝试对所有分配给当前consumer的队列请求broker端的消息队列锁，保证同时只有一个消费端可以消费。
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        ConsumeMessageOrderlyService.this.lockMQPeriodically();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
                    }
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            //锁定所有消息队列
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    @Override
    public void shutdown(long awaitTerminateMillis) {

    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {

    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return 0;
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        return null;
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {

    }

//    @Override
//    public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue processQueue, MessageQueue messageQueue) {
//
//    }
}
